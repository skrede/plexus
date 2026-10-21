#include "test_serial_common.h"
#include "drain_after_destroy_common.h"

#include "support/retire_drain.h"

#include "plexus/stream/crc_serial.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <sys/ioctl.h>
#include <unistd.h>

#include <span>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace serial_fixture;
using drain_after_destroy::drain;
using drain_after_destroy::k_iterations;
using drain_after_destroy::poll_one_until;
using plexus::testing::retire_and_drain;

namespace {

std::vector<std::byte> serial_frame()
{
    wire::frame_header hdr{};
    hdr.type = wire::msg_type::unidirectional;
    const std::vector<std::byte> body(16, std::byte{0x42});
    auto out            = wire::encode_frame(hdr, body);
    const auto framed   = std::span<const std::byte>{out};
    const auto trailer  = stream::crc_trailer(framed.first(wire::header_size), framed.subspan(wire::header_size));
    out.insert(out.end(), trailer.begin(), trailer.end());
    return out;
}

std::vector<std::byte> concat(std::vector<std::byte> first, const std::vector<std::byte> &second)
{
    first.insert(first.end(), second.begin(), second.end());
    return first;
}

bool wait_readable(int fd, std::size_t bytes)
{
    const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int available    = 0;
    while(std::chrono::steady_clock::now() < bound)
    {
        if(::ioctl(fd, FIONREAD, &available) == 0 && static_cast<std::size_t>(available) >= bytes)
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

// The bytes reach the line before the channel adopts it, so the read its constructor starts completes at once and that
// completion is queued ahead of anything the test does next.
std::unique_ptr<pasio::serial_channel> adopt_with_bytes_queued(::asio::io_context &io, pty_pair &pty, std::span<const std::byte> bytes)
{
    REQUIRE(::write(pty.master, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size()));
    REQUIRE(wait_readable(pty.slave, bytes.size()));
    return adopt_channel(io, pty.take_slave());
}

}

TEST_CASE("serial_retire_drain a verified frame queued before close is not handed to the consumer", "[integration][serial][teardown][!shouldfail]")
{
    const auto bytes = concat(serial_frame(), serial_frame());
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        auto ch                = adopt_with_bytes_queued(io, pty, bytes);
        std::int32_t delivered = 0;
        ch->on_data([&](std::span<const std::byte>) { ++delivered; });
        REQUIRE(poll_one_until(io, [&] { return delivered > 0; }));
        const auto outcome = retire_and_drain(ch, [&] { drain(io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE(delivered == 1);
    }
}

// The read completed with a verified frame before the retire, so its completion is still queued when the channel
// closes: the channel may be erased only after it ran and nothing it could have posted is left.
TEST_CASE("serial_retire_drain a retired channel is destroyed only after its pending read drained", "[integration][serial][teardown]")
{
    const auto bytes = serial_frame();
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        auto ch                = adopt_with_bytes_queued(io, pty, bytes);
        std::int32_t delivered = 0;
        ch->on_data([&](std::span<const std::byte>) { ++delivered; });
        const auto outcome = retire_and_drain(ch, [&] { drain(io); });
        REQUIRE(outcome.erased == 1);
        REQUIRE(outcome.empty);
        REQUIRE(delivered == 0);
    }
}

TEST_CASE("serial_retire_drain bytes read after close reach neither on_data nor on_frame_dropped", "[integration][serial][teardown][!shouldfail]")
{
    auto corrupt = serial_frame();
    corrupt[wire::header_size + 2] ^= std::byte{0xFF};
    const auto bytes = concat(corrupt, serial_frame());
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        auto ch                = adopt_with_bytes_queued(io, pty, bytes);
        std::int32_t delivered = 0;
        std::int32_t dropped   = 0;
        ch->on_data([&](std::span<const std::byte>) { ++delivered; });
        ch->on_frame_dropped([&](wire::close_cause) { ++dropped; });
        ch->close();
        drain(io);
        REQUIRE(dropped == 0);
        REQUIRE(delivered == 0);
    }
}
