#include "test_serial_common.h"

#include "support/serial_kat.h"

#include "plexus/wire/frame_codec.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <vector>
#include <cstddef>

using namespace serial_fixture;

namespace {

constexpr std::chrono::seconds k_egress_bound{1};

void make_nonblocking(int fd)
{
    REQUIRE(::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK) == 0);
}

void read_available(int fd, std::vector<std::byte> &into)
{
    std::array<std::byte, 64> chunk{};
    for(ssize_t n = ::read(fd, chunk.data(), chunk.size()); n > 0; n = ::read(fd, chunk.data(), chunk.size()))
        into.insert(into.end(), chunk.begin(), chunk.begin() + n);
}

}

TEST_CASE("serial egress: the host channel writes the pinned serial frame bytes", "[integration][serial][kat]")
{
    const auto &row     = plexus::testing::serial_kat_row("two-byte payload");
    const auto expected = plexus::testing::from_hex(row.frame_hex);
    pty_pair pty;
    make_nonblocking(pty.master);
    ::asio::io_context io;
    auto tx          = adopt_channel(io, pty.take_slave());
    const auto frame = wire::encode_frame(row.header, plexus::testing::from_hex(row.payload_hex));
    tx->send(frame);

    std::vector<std::byte> written;
    plexus::testing::pump_until(
            io,
            [&]
            {
                read_available(pty.master, written);
                return written.size() >= expected.size();
            },
            k_egress_bound);
    REQUIRE(written == expected);
}
