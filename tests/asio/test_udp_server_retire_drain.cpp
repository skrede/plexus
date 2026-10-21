#include "support/alloc_watch.h"

#include "plexus/asio/udp_server.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/udp.hpp>
#include <asio/io_context.hpp>

#include <vector>
#include <cstddef>
#include <cstdint>

namespace pasio = plexus::asio;

namespace {

constexpr std::size_t k_datagram_bytes = 1531;

const ::asio::ip::udp::endpoint k_any_loopback{::asio::ip::make_address("127.0.0.1"), 0};

void drain(::asio::io_context &io)
{
    for(std::int32_t i = 0; i < 64; ++i)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

TEST_CASE("udp_server_retire_drain a live close keeps the in-flight datagram until its send completed", "[udp][teardown][!shouldfail]")
{
    ::asio::io_context io;
    ::asio::ip::udp::socket peer{io, k_any_loopback};
    pasio::udp_server server{io};
    server.start(k_any_loopback);

    const std::vector<std::byte> datagram(k_datagram_bytes, std::byte{0x5A});
    plexus::testing::watch_next_alloc(k_datagram_bytes);
    server.send_to(datagram, peer.local_endpoint());
    REQUIRE(plexus::testing::watched_allocated());

    server.close();
    REQUIRE_FALSE(plexus::testing::watched_freed());
    drain(io);
    REQUIRE(plexus::testing::watched_freed());
}
