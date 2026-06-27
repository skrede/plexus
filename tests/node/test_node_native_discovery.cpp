#include "test_node_native_discovery_common.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/node_id.h"
#include "plexus/discovery/contact_card.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <string>
#include <cstdint>

using namespace node_native_discovery_fixture;

namespace {

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0xa5)};
    return id;
}

} // namespace

// The deterministic equivalent of the live slice: two real nodes, each with a multicast_discovery
// over a paired wiring socket. It exercises the full advertise -> announcement -> decode ->
// note_from_card -> note_peer path with no real multicast, so it runs in the default set. Both
// nodes end aware of each other (records only — note_peer is awareness, no dial issued by
// discovery) with metadata byte-identical to assemble_contact_card.
TEST_CASE("node_native_discovery: two nodes discover each other over the wired datagram seam", "[node_native_discovery]")
{
    ::asio::io_context io;

    wiring_datagram_socket sock_a{io, "127.0.1.1"};
    wiring_datagram_socket sock_b{io, "127.0.1.2"};
    sock_a.pair_with(sock_b);
    sock_b.pair_with(sock_a);

    plexus::native::multicast_discovery<wiring_datagram_socket, pasio::asio_policy> disc_a{io, sock_a};
    plexus::native::multicast_discovery<wiring_datagram_socket, pasio::asio_policy> disc_b{io, sock_b};

    pasio::asio_transport transport_a{io};
    pasio::asio_transport transport_b{io};

    const auto id_a = make_id(0xA1);
    const auto id_b = make_id(0xB2);
    asio_node a{io, disc_a, id_a, transport_a, make_opts()};
    asio_node b{io, disc_b, id_b, transport_b, make_opts()};

    a.listen({"tcp", "127.0.1.1:18801"});
    b.listen({"tcp", "127.0.1.2:18802"});

    pump_until(io, [&] { return a.router().known().contains(id_b) && b.router().known().contains(id_a); });

    REQUIRE(a.router().known().contains(id_b));
    REQUIRE(b.router().known().contains(id_a));

    const auto ep_b = a.router().known().lookup(id_b);
    REQUIRE(ep_b.has_value());
    REQUIRE(ep_b->scheme == "tcp");
    REQUIRE(ep_b->address == "127.0.1.2:18802");

    const auto ep_a = b.router().known().lookup(id_a);
    REQUIRE(ep_a.has_value());
    REQUIRE(ep_a->address == "127.0.1.1:18801");

    // Discovery records awareness only; it never dials. A noted peer carries no live session.
    REQUIRE(a.router().session_for(id_b) == nullptr);
    REQUIRE(b.router().session_for(id_a) == nullptr);
}

// The live slice over the real udp_multicast_socket. Hidden behind [.multicast]: same-host
// multicast loopback is environment-sensitive (a sandbox may drop IGMP-joined delivery), so the
// deterministic wired equivalent above carries the default-set proof. This is NEVER weakened to a
// 127.0.0.1 unicast send. A test-local group avoids colliding with a co-resident plexus default.
TEST_CASE("node_native_discovery: two nodes discover each other over real multicast", "[.multicast]")
{
    ::asio::io_context io;
    const auto group = ::asio::ip::make_address_v4("239.255.0.73");
    constexpr std::uint16_t mc_port = 17453;
    constexpr unsigned mc_ttl       = 1;

    pasio::udp_multicast_socket sock_a{io, group, mc_port, mc_ttl};
    pasio::udp_multicast_socket sock_b{io, group, mc_port, mc_ttl};

    plexus::native::discovery_options opts;
    opts.group           = "239.255.0.73";
    opts.port            = mc_port;
    opts.ttl             = mc_ttl;
    opts.announce_period = std::chrono::milliseconds(100);

    plexus::native::multicast_discovery<pasio::udp_multicast_socket, pasio::asio_policy> disc_a{io, sock_a, opts};
    plexus::native::multicast_discovery<pasio::udp_multicast_socket, pasio::asio_policy> disc_b{io, sock_b, opts};

    pasio::asio_transport transport_a{io};
    pasio::asio_transport transport_b{io};

    const auto id_a = make_id(0xC3);
    const auto id_b = make_id(0xD4);
    asio_node a{io, disc_a, id_a, transport_a, make_opts()};
    asio_node b{io, disc_b, id_b, transport_b, make_opts()};

    a.listen({"tcp", "127.0.0.1:18811"});
    b.listen({"tcp", "127.0.0.1:18812"});

    pump_until(io, [&] { return a.router().known().contains(id_b) && b.router().known().contains(id_a); });

    REQUIRE(a.router().known().contains(id_b));
    REQUIRE(b.router().known().contains(id_a));
    // The host comes from the datagram's kernel source IP (unspoofable, not a wire field), so only
    // the advertised port key is fixed; the address ends with the listened port.
    const auto ep_a = b.router().known().lookup(id_a);
    REQUIRE(ep_a.has_value());
    REQUIRE(ep_a->scheme == "tcp");
    REQUIRE(ep_a->address.ends_with(":18811"));
}
