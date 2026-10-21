#include "support/alloc_watch.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_multicast_socket.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/udp.hpp>
#include <asio/ip/address_v4.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace pasio = plexus::asio;

namespace {

constexpr std::size_t k_datagram_bytes = 1531;

const ::asio::ip::udp::endpoint k_any_loopback{::asio::ip::make_address("127.0.0.1"), 0};

// Distinct from the multicast socket's own suite so the two never share a group when run in parallel.
const ::asio::ip::address_v4 k_test_group = ::asio::ip::make_address_v4("239.255.0.72");
constexpr std::uint16_t k_test_port       = 17448;

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

TEST_CASE("udp_server_retire_drain a live close keeps the in-flight datagram until its send completed", "[udp][teardown]")
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

TEST_CASE("udp_server_retire_drain a closed server reports no outstanding operation once its aborted receive drained", "[udp][teardown]")
{
    ::asio::io_context io;
    auto server = std::make_unique<pasio::udp_server>(io);
    server->start(k_any_loopback);
    REQUIRE(server->outstanding_ops() == 1);

    server->close();
    REQUIRE(server->outstanding_ops() == 1);
    drain(io);
    REQUIRE(server->outstanding_ops() == 0);

    server.reset();
    drain(io);
}

TEST_CASE("udp_server_retire_drain a closed server reports no outstanding operation once its in-flight send drained", "[udp][teardown]")
{
    ::asio::io_context io;
    ::asio::ip::udp::socket peer{io, k_any_loopback};
    auto server = std::make_unique<pasio::udp_server>(io);
    server->start(k_any_loopback);

    const std::vector<std::byte> datagram(k_datagram_bytes, std::byte{0x5A});
    server->send_to(datagram, peer.local_endpoint());
    REQUIRE(server->outstanding_ops() == 2);

    server->close();
    REQUIRE(server->outstanding_ops() == 2);
    drain(io);
    REQUIRE(server->outstanding_ops() == 0);
    REQUIRE(server->queued_send_bytes() == 0);

    server.reset();
    drain(io);
}

// The socket has no bind without its group join, so this binds and joins the test group.
TEST_CASE("udp_server_retire_drain a closed multicast socket reports no outstanding operation once drained", "[udp][teardown]")
{
    ::asio::io_context io;
    auto socket = std::make_unique<pasio::udp_multicast_socket>(io, k_test_group, k_test_port, 1u);
    REQUIRE_FALSE(socket->bind({::asio::ip::udp::v4(), k_test_port}));

    const std::vector<std::byte> datagram(k_datagram_bytes, std::byte{0x5A});
    socket->send_multicast(datagram);
    REQUIRE(socket->outstanding_ops() == 2);

    socket->close();
    REQUIRE(socket->outstanding_ops() == 2);
    drain(io);
    REQUIRE(socket->outstanding_ops() == 0);

    socket.reset();
    drain(io);
}
