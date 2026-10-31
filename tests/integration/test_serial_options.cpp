#include "test_serial_common.h"

#include "plexus/asio/serial_transport.h"

#include "plexus/stream/serial_frame.h"

#include "plexus/io/congestion.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/serial_port.hpp>

#include <unistd.h>

#include <memory>
#include <string>
#include <cstddef>
#include <utility>

using namespace serial_fixture;

namespace {

constexpr std::size_t k_serial_ceiling = stream::serial_frame_ceiling(stream::k_serial_default_max_payload);

std::size_t listened_frame_bytes(pasio::serial_transport &transport)
{
    pty_pair pty;
    std::unique_ptr<pasio::serial_channel> delivered;
    transport.on_accepted([&delivered](std::unique_ptr<pasio::serial_channel> ch) { delivered = std::move(ch); });
    transport.listen({"serial", std::string{::ttyname(pty.slave)} + "@115200"});
    REQUIRE(delivered);
    return delivered->max_frame_bytes();
}

}

TEST_CASE("serial options: a channel given only a congestion override keeps the serial cap", "[integration][serial][options]")
{
    pty_pair pty;
    ::asio::io_context io;
    pasio::serial_channel ch{io, ::asio::serial_port{io, pty.take_slave()}, {.congestion = pio::congestion::drop_newest}};

    CHECK(ch.congestion_mode() == pio::congestion::drop_newest);
    CHECK(ch.max_frame_bytes() == k_serial_ceiling);
}

TEST_CASE("serial options: the test helper's channel runs at the production cap", "[integration][serial][options]")
{
    pty_pair pty;
    ::asio::io_context io;
    auto ch = adopt_channel(io, pty.take_slave());

    CHECK(ch->max_frame_bytes() == k_serial_ceiling);
}

TEST_CASE("serial options: a listened transport channel carries the transport's cap", "[integration][serial][options]")
{
    constexpr std::size_t k_override = 4096;
    ::asio::io_context io;
    pasio::serial_transport defaulted{io};
    pasio::serial_transport overridden{io, {.max_payload = k_override}};

    CHECK(listened_frame_bytes(defaulted) == k_serial_ceiling);
    CHECK(listened_frame_bytes(overridden) == stream::serial_frame_ceiling(k_override));
}
