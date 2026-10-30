#include "test_serial_rx_probe.h"

#include "support/serial_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <cstddef>
#include <cstdint>

using namespace serial_fixture;
using plexus::testing::k_serial_header_bytes;

TEST_CASE("serial resync: a byte-FLIPPED frame is dropped and the link recovers on the next frame, "
          "looped",
          "[integration][serial][resync]")
{
    constexpr std::int32_t k_iterations = 25;
    std::int32_t recovered              = 0;
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        const int master = pty.take_master();
        auto rx          = adopt_channel(io, pty.take_slave());

        rx_probe probe;
        probe.wire(*rx);

        auto bad = serial_data_frame("corrupt-flip", 1);
        bad[k_serial_header_bytes + 2] ^= std::byte{0xFF};
        write_raw(master, bad);
        write_raw(master, serial_data_frame("recovered-after-flip", 1));

        plexus::testing::pump_until(io, [&] { return probe.received.has_value(); }, k_pump_bound);

        REQUIRE(probe.drops >= 1);
        REQUIRE(probe.received.has_value());
        REQUIRE(*probe.received == "recovered-after-flip");
        REQUIRE_FALSE(probe.protocol_closed);
        REQUIRE_FALSE(probe.closed);
        ++recovered;
        ::close(master);
    }
    REQUIRE(recovered == k_iterations);
}

TEST_CASE("serial resync: a byte-DROPPED (mid-frame) frame desyncs but the next frame resyncs, "
          "looped",
          "[integration][serial][resync]")
{
    constexpr std::int32_t k_iterations = 25;
    std::int32_t recovered              = 0;
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        const int master = pty.take_master();
        auto rx          = adopt_channel(io, pty.take_slave());

        rx_probe probe;
        probe.wire(*rx);

        // A payload byte is dropped, not the final trailer byte: the next frame's first anchor byte would complete that
        // trailer and pass the check about one time in 256.
        auto dropped = serial_data_frame("corrupt-drop", 1);
        dropped.erase(dropped.begin() + k_serial_header_bytes + 1);
        write_raw(master, dropped);
        write_raw(master, serial_data_frame("recovered-after-drop", 1));

        plexus::testing::pump_until(io, [&] { return probe.received.has_value(); }, k_pump_bound);

        REQUIRE(probe.received.has_value());
        REQUIRE(*probe.received == "recovered-after-drop");
        REQUIRE_FALSE(probe.protocol_closed);
        REQUIRE_FALSE(probe.closed);
        ++recovered;
        ::close(master);
    }
    REQUIRE(recovered == k_iterations);
}

TEST_CASE("serial resync: corruption is non-fatal - neither on_protocol_close nor on_closed fires", "[integration][serial][resync]")
{
    constexpr std::int32_t k_iterations = 25;
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        const int master = pty.take_master();
        auto rx          = adopt_channel(io, pty.take_slave());

        rx_probe probe;
        probe.wire(*rx);

        auto bad1 = serial_data_frame("bad-one", 1);
        bad1.back() ^= std::byte{0x5A};
        auto bad2 = serial_data_frame("bad-two", 1);
        bad2[k_serial_header_bytes] ^= std::byte{0x33};
        write_raw(master, bad1);
        write_raw(master, bad2);
        write_raw(master, serial_data_frame("survivor", 1));

        plexus::testing::pump_until(io, [&] { return probe.received.has_value(); }, k_pump_bound);

        REQUIRE(probe.received.has_value());
        REQUIRE(*probe.received == "survivor");
        REQUIRE(probe.drops >= 1);
        REQUIRE_FALSE(probe.protocol_closed);
        REQUIRE_FALSE(probe.closed);
        ::close(master);
    }
}
