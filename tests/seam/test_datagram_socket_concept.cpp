#include "plexus/asio/udp_multicast_socket.h"

#include "plexus/stream/datagram_socket.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

namespace {

namespace pasio = plexus::asio;

// The from-scratch build must actually compile udp_multicast_socket.h here (the false-green lesson:
// a concept assertion only proves the header if the TU that holds it is built).
static_assert(plexus::stream::datagram_socket<pasio::udp_multicast_socket>);

TEST_CASE("a udp_multicast_socket constructs, binds + joins the group, and closes with no error", "[datagram_socket][seam]")
{
    ::asio::io_context io;
    pasio::udp_multicast_socket sock{io, ::asio::ip::make_address_v4("239.255.0.7"), 7447, 4};

    std::error_code ec = sock.bind({::asio::ip::udp::v4(), 7447});
    REQUIRE_FALSE(ec);

    sock.close();
}

}
