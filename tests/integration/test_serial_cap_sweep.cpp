#include "test_serial_common.h"
#include "test_serial_rx_probe.h"

#include "support/serial_bytes.h"

#include "plexus/io/fragmentation.h"
#include "plexus/io/liveliness_options.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <unistd.h>

#include <span>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <algorithm>

using namespace serial_fixture;
using plexus::testing::k_serial_timestamp_offset;
using clock_type = std::chrono::steady_clock;
using double_ms  = std::chrono::duration<double, std::milli>;

namespace {

const std::chrono::milliseconds k_interval = pio::liveliness_options{}.heartbeat_interval;

constexpr std::uint32_t k_rate_115200 = 11520;
constexpr std::uint32_t k_rate_9600   = 960;
constexpr std::size_t k_chunk_bytes   = 32;
constexpr std::size_t k_small_bytes   = 16;
constexpr std::size_t k_sweep_cap     = 64 * 1024;
constexpr std::int32_t k_min_frames   = 5;
constexpr std::chrono::seconds k_cell_span{3};
constexpr std::chrono::seconds k_drive_limit{30};
constexpr std::chrono::milliseconds k_poll_slice{5};
constexpr std::array<std::size_t, 9> k_fast_payloads{256, 512, 768, 1024, 1280, 1536, 2048, 4096, 8192};
constexpr std::array<std::size_t, 4> k_slow_payloads{64, 128, 256, 512};
constexpr std::array<std::size_t, 3> k_hold_payloads{64, 256, 1024};

std::vector<std::byte> frame_of_payload_len(std::size_t payload_len, char fill)
{
    const std::size_t overhead = make_data_frame("", 1).size() - wire::header_size;
    return serial_data_frame(std::string(payload_len - overhead, fill), 1);
}

// A token bucket refilled at the line rate, holding two chunks so a late wake-up does not lower the rate.
struct paced_line
{
    bool write(std::span<const std::byte> frame)
    {
        for(std::size_t at = 0; at < frame.size(); at += k_chunk_bytes)
        {
            const std::span<const std::byte> chunk = frame.subspan(at, std::min(k_chunk_bytes, frame.size() - at));
            const auto need                        = static_cast<double>(chunk.size());
            if(refill() < need)
                std::this_thread::sleep_for(std::chrono::duration<double>((need - tokens) / rate));
            tokens = refill() - need;
            if(::write(fd, chunk.data(), chunk.size()) != static_cast<ssize_t>(chunk.size()))
                return false;
        }
        return true;
    }

    double refill()
    {
        const clock_type::time_point now = clock_type::now();
        tokens                           = std::min(2.0 * k_chunk_bytes, tokens + std::chrono::duration<double>(now - refilled).count() * rate);
        refilled                         = now;
        return tokens;
    }

    int fd;
    std::uint32_t rate;
    double tokens{0.0};
    clock_type::time_point refilled{clock_type::now()};
};

// A heartbeat tick that elapses while a long frame is on the line coalesces into one small frame at the next boundary.
bool feed_sweep(paced_line line, std::span<const std::byte> data, std::span<const std::byte> small)
{
    const clock_type::time_point start = clock_type::now();
    clock_type::time_point next_small  = start + k_interval;
    bool ok                            = true;
    for(std::int32_t sent = 0; ok && (clock_type::now() - start < k_cell_span || sent < k_min_frames);)
    {
        const bool small_due = clock_type::now() >= next_small;
        ok                   = line.write(small_due ? small : data);
        sent += small_due ? 0 : 1;
        while(small_due && next_small <= clock_type::now())
            next_small += k_interval;
    }
    return ok;
}

bool feed_hold(paced_line line, std::span<const std::byte> bad, std::span<const std::byte> good, const std::atomic<bool> &stop)
{
    bool ok = line.write(bad);
    while(ok && !stop)
        ok = line.write(good);
    return ok;
}

double max_gap_ms(const std::vector<rx_delivery> &deliveries, std::optional<std::size_t> only_bytes)
{
    std::vector<clock_type::time_point> at;
    for(const rx_delivery &d : deliveries)
        if(!only_bytes || d.data_bytes == *only_bytes)
            at.push_back(d.at);
    double worst = 0.0;
    for(std::size_t i = 1; i < at.size(); ++i)
        worst = std::max(worst, double_ms(at[i] - at[i - 1]).count());
    return worst;
}

struct pty_rig
{
    explicit pty_rig(std::size_t cap)
            : rx(adopt_channel(io, pty.take_slave(), stream::with_message_limits({}, cap, cap + wire::header_size)))
    {
        probe.wire(*rx);
    }

    template<typename Feed, typename Done>
    bool run(Feed feed, Done done)
    {
        std::atomic<bool> stop{false};
        std::future<bool> writer              = std::async(std::launch::async, [&] { return feed(stop); });
        const clock_type::time_point deadline = clock_type::now() + k_drive_limit;
        while(writer.wait_for(std::chrono::seconds{0}) != std::future_status::ready && !done() && clock_type::now() < deadline)
            io.run_for(k_poll_slice);
        stop = true;
        return writer.get();
    }

    pty_pair pty;
    ::asio::io_context io;
    std::unique_ptr<pasio::serial_channel> rx;
    rx_probe probe;
};

void report_sweep_cell(std::uint32_t rate, std::size_t payload_len)
{
    pty_rig rig{k_sweep_cap};
    const std::vector<std::byte> data  = frame_of_payload_len(payload_len, 'd');
    const std::vector<std::byte> small = serial_data_frame(std::string(k_small_bytes, 'h'), 1);
    REQUIRE(rig.run([&](const std::atomic<bool> &) { return feed_sweep(paced_line{rig.pty.master, rate}, data, small); }, [] { return false; }));
    rig.io.run_for(k_interval);
    std::cout << "SWEEP rate=" << rate << " payload=" << payload_len << " max_gap_ms=" << max_gap_ms(rig.probe.deliveries, std::nullopt)
              << " hb_max_gap_ms=" << max_gap_ms(rig.probe.deliveries, k_small_bytes) << std::endl;
}

// The rig keeps the 8 MiB cap the node path gives the serial leg, so the stretched length is awaited, not refused.
void report_hold(std::size_t payload_len)
{
    pty_rig rig{plexus::io::global_default_max_message_bytes};
    const std::vector<std::byte> good = frame_of_payload_len(payload_len, 'g');
    std::vector<std::byte> bad        = good;
    bad.erase(bad.begin() + static_cast<std::ptrdiff_t>(k_serial_timestamp_offset));
    const clock_type::time_point started = clock_type::now();
    REQUIRE(rig.run([&](const std::atomic<bool> &stop) { return feed_hold(paced_line{rig.pty.master, k_rate_115200}, bad, good, stop); },
                    [&] { return !rig.probe.deliveries.empty(); }));
    std::cout << "HOLD payload=" << payload_len << " first_delivery_ms=";
    if(rig.probe.deliveries.empty())
        std::cout << "held_over_30s" << std::endl;
    else
        std::cout << double_ms(rig.probe.deliveries.front().at - started).count() << std::endl;
}

}

TEST_CASE("serial cap sweep: delivery gaps of back-to-back frames on a paced line", "[.][sweep]")
{
    for(std::size_t payload_len : k_fast_payloads)
        report_sweep_cell(k_rate_115200, payload_len);
    for(std::size_t payload_len : k_slow_payloads)
        report_sweep_cell(k_rate_9600, payload_len);
}

TEST_CASE("serial header drop latency: the first delivery after a dropped header byte on a paced line", "[.][header-drop-latency]")
{
    for(std::size_t payload_len : k_hold_payloads)
        report_hold(payload_len);
}
