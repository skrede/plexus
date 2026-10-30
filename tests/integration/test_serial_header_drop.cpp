#include "test_serial_common.h"
#include "test_serial_rx_probe.h"

#include "support/serial_bytes.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/io/liveness_event.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/liveliness_options.h"

#include "plexus/testing/pump.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <unistd.h>

#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <system_error>

using namespace serial_fixture;
using plexus::testing::k_serial_header_bytes;
using plexus::testing::k_serial_timestamp_offset;

namespace {

using monitor_type = pio::liveliness_monitor<pasio::asio_policy>;

const std::chrono::milliseconds k_interval = pio::liveliness_options{}.heartbeat_interval;
const plexus::node_id k_peer{};

constexpr std::int32_t k_interval_frames    = 10;
constexpr std::int32_t k_liveliness_rounds  = 3;
constexpr std::size_t k_min_delivered_count = 9;

std::vector<std::byte> without_byte(std::vector<std::byte> bytes, std::size_t offset)
{
    bytes.erase(bytes.begin() + static_cast<std::ptrdiff_t>(offset));
    return bytes;
}

std::uint64_t default_lease_ns()
{
    const pio::liveliness_options opts;
    return static_cast<std::uint64_t>(std::chrono::nanoseconds(opts.heartbeat_interval * opts.heartbeat_miss_limit).count());
}

void expect_next_frame_at_once(std::size_t offset)
{
    pty_pair pty;
    ::asio::io_context io;
    const int master = pty.take_master();
    auto rx          = adopt_channel(io, pty.take_slave());
    rx_probe probe;
    probe.wire(*rx);
    write_raw(master, without_byte(serial_data_frame("frame-a", 1), offset));
    write_raw(master, serial_data_frame("frame-b", 1));
    plexus::testing::pump_until(io, [&] { return probe.received.has_value(); }, k_interval);
    CHECK(probe.received.value_or("<nothing>") == "frame-b");
    REQUIRE_FALSE(probe.protocol_closed);
    REQUIRE_FALSE(probe.closed);
    ::close(master);
}

// The second frame loses a timestamp byte, so its declared length reads about 256 times its real one; with at least
// 16 payload bytes that exceeds everything the remaining frames carry.
class interval_writer
{
public:
    interval_writer(::asio::io_context &io, int fd)
            : m_fd(fd)
            , m_written(0)
            , m_timer(io)
            , m_next(std::chrono::steady_clock::now())
    {
    }

    void arm()
    {
        m_next += k_interval;
        m_timer.expires_at(m_next);
        m_timer.async_wait(
                [this](std::error_code ec)
                {
                    if(!ec)
                        write_next();
                });
    }

private:
    int m_fd;
    std::int32_t m_written;
    ::asio::steady_timer m_timer;
    std::chrono::steady_clock::time_point m_next;

    void write_next()
    {
        const auto frame = serial_data_frame("liveliness-frame-" + std::to_string(m_written), 1);
        write_raw(m_fd, m_written == 1 ? without_byte(frame, k_serial_timestamp_offset) : frame);
        if(++m_written < k_interval_frames)
            arm();
    }
};

void watch_peer(monitor_type &monitor, std::int32_t &dead_edges)
{
    monitor.register_endpoint(k_peer, 0, 0, default_lease_ns());
    monitor.on_liveness([&dead_edges](const pio::liveness_event &) { ++dead_edges; });
    monitor.start();
}

void drain_monitor(monitor_type &monitor, ::asio::io_context &io)
{
    monitor.shutdown();
    plexus::testing::pump_until(io, [&] { return monitor.pending().settled(); }, k_pump_bound);
}

void expect_peer_kept_alive()
{
    pty_pair pty;
    ::asio::io_context io;
    const int master = pty.take_master();
    auto rx          = adopt_channel(io, pty.take_slave());
    monitor_type monitor{io};
    std::int32_t dead_edges = 0;
    watch_peer(monitor, dead_edges);
    rx_probe probe;
    probe.delivered_cb = [&] { monitor.stamp_seen(k_peer); };
    probe.wire(*rx);
    interval_writer writer{io, master};
    writer.arm();
    io.run_for(k_interval * (k_interval_frames + 2));
    CHECK(dead_edges == 0);
    CHECK(probe.deliveries.size() >= k_min_delivered_count);
    drain_monitor(monitor, io);
    ::close(master);
}

}

TEST_CASE("serial header drop: a dropped header byte costs only its frame and the next frame arrives at once", "[integration][serial][!shouldfail]")
{
    for(std::size_t offset = 0; offset < k_serial_header_bytes; ++offset)
    {
        INFO("erased header offset " << offset);
        expect_next_frame_at_once(offset);
    }
}

TEST_CASE("serial header drop: liveliness never declares the peer dead across a dropped header byte", "[integration][serial][!shouldfail]")
{
    for(std::int32_t round = 0; round < k_liveliness_rounds; ++round)
    {
        INFO("round " << round);
        expect_peer_kept_alive();
    }
}
