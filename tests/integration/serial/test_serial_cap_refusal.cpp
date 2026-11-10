#include "test_serial_common.h"

#include "support/serial_bytes.h"

#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/close_cause.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

using namespace serial_fixture;
using plexus::testing::k_serial_timestamp_offset;

namespace {

constexpr std::int32_t k_runs         = 3;
constexpr std::size_t k_sender_cap    = 4096;
constexpr std::size_t k_over_cap_body = 2048;
constexpr std::size_t k_fitting_body  = 16;

// A fixed timestamp fixes every byte of the frame, so no run can meet a stray format id in a refused header or trailer.
std::vector<std::byte> fixed_frame(const std::string &body)
{
    auto frame = make_data_frame(body, 1);
    std::fill_n(frame.begin() + k_serial_timestamp_offset, sizeof(std::uint64_t), std::byte{0});
    return frame;
}

struct cap_probe
{
    explicit cap_probe(pasio::serial_channel &rx)
            : over_cap(0)
            , crc_mismatch(0)
            , sink()
            , router(sink)
            , received()
    {
        router.on_unidirectional([this](const wire::frame_header &, std::span<const std::byte> inner) { deliver(inner); });
        rx.on_data([this](std::span<const std::byte> f) { router.route(f); });
        rx.on_frame_dropped([this](wire::close_cause c) { count(c); });
    }

    std::int32_t over_cap;
    std::int32_t crc_mismatch;
    plexus::log::null_logger sink;
    pio::frame_router router;
    std::optional<std::string> received;

private:
    void deliver(std::span<const std::byte> inner)
    {
        const auto decoded = wire::decode_unidirectional(inner);
        if(decoded)
            received = to_string(decoded->data);
    }

    void count(wire::close_cause c)
    {
        over_cap += c == wire::close_cause::payload_too_large;
        crc_mismatch += c == wire::close_cause::crc_mismatch;
    }
};

}

TEST_CASE("serial cap refusal: a frame over the receiver's cap from a wider peer is reported as payload_too_large and "
          "the next frame is delivered",
          "[integration][serial][cap]")
{
    const std::string fitting(k_fitting_body, '\x22');
    const auto over_cap_frame = fixed_frame(std::string(k_over_cap_body, '\x11'));
    const auto fitting_frame  = fixed_frame(fitting);
    std::int32_t proven       = 0;
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        pty_pair pty;
        ::asio::io_context io;
        auto tx = adopt_channel(io, pty.take_master(), {.max_payload = k_sender_cap});
        auto rx = adopt_channel(io, pty.take_slave());
        cap_probe probe{*rx};

        tx->send(over_cap_frame);
        tx->send(fitting_frame);
        plexus::testing::pump_until(io, [&] { return probe.received.has_value(); }, k_pump_bound);

        REQUIRE(probe.over_cap == 1);
        REQUIRE(probe.crc_mismatch == 0);
        REQUIRE(probe.received == fitting);
        ++proven;
    }
    REQUIRE(proven == k_runs);
}
