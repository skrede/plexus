#include "plexus/asio/udp_server.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/asio_listener.h"
#include "plexus/asio/unix_listener.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/serial_channel.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include "plexus/io/congestion.h"

#include "plexus/io/security/peer_cred_policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/ip/address_v4.hpp>

#include <asio/local/stream_protocol.hpp>

#include <stdexcept>

namespace {

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

constexpr pio::congestion k_drop_oldest = pio::congestion::drop_oldest;

}

TEST_CASE("asio congestion: a TCP or serial channel, transport or listener configured to drop its oldest frames is refused when built", "[asio][congestion]")
{
    ::asio::io_context io;
    CHECK_THROWS_AS(pasio::asio_channel(io, {}, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::asio_channel(io, ::asio::ip::tcp::socket{io}, {}, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::asio_transport(io, {}, true, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::asio_listener(io, {}, true, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::serial_channel(io, pasio::serial_channel_options{.congestion = k_drop_oldest}), std::runtime_error);
}

TEST_CASE("asio congestion: an AF_UNIX channel, transport or listener configured to drop its oldest frames is refused when built", "[asio][congestion]")
{
    ::asio::io_context io;
    const auto &any_peer = pio::security::shared_accept_any_peer_cred();
    CHECK_THROWS_AS(pasio::unix_channel(io, {}, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::unix_channel(io, ::asio::local::stream_protocol::socket{io}, {}, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::unix_transport(io, {}, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::unix_listener(io, {}, pasio::unix_listener::default_socket_mode, any_peer, k_drop_oldest), std::runtime_error);
}

TEST_CASE("asio congestion: a UDP server or multicast socket configured to drop its oldest frames is refused when built", "[asio][congestion]")
{
    ::asio::io_context io;
    const ::asio::ip::address_v4 group = ::asio::ip::make_address_v4("239.255.0.1");
    CHECK_THROWS_AS(pasio::udp_server(io, k_drop_oldest), std::runtime_error);
    CHECK_THROWS_AS(pasio::udp_multicast_socket(io, group, 0, 1, k_drop_oldest), std::runtime_error);
}
