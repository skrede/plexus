#include "test_multicast_discovery_common.h"

#include "plexus/discovery/detail/announcement_card.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"
#include "plexus/discovery/universe.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/discovery_options.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace multicast_discovery_fixture;

namespace {

constexpr std::uint32_t universe_a = plexus::discovery::universe_from_label("warehouse-fleet");
constexpr std::uint32_t universe_b = plexus::discovery::universe_from_label("dockyard-fleet");

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0xff)};
    return id;
}

std::vector<plexus::discovery::listening_transport> sample_listens()
{
    return {{"tcp", 7000}, {"udp", 7100}};
}

std::vector<std::byte> inbound_bytes(std::uint8_t seed, std::uint32_t universe, std::uint8_t flags = 0)
{
    return plexus::wire::encode_announcement(
            plexus::discovery::detail::announcement_from_card(make_id(seed), sample_listens(), 30, flags, universe));
}

plexus::discovery::discovery_options options_for(std::uint32_t universe)
{
    plexus::discovery::discovery_options opts;
    opts.universe = universe;
    return opts;
}

} // namespace

TEST_CASE("multicast_discovery: a foreign-universe announcement fires no on_resolved", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for(universe_a)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    socket.inject("10.0.0.9", inbound_bytes(0x21, universe_b));

    REQUIRE(resolved == 0);
}

TEST_CASE("multicast_discovery: a browse-only node filters a foreign universe", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for(universe_a)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    // No advertise: m_announcement stays nullopt, so the compare keys off m_options.universe.
    socket.inject("10.0.0.9", inbound_bytes(0x22, universe_b));

    REQUIRE(resolved == 0);
}

TEST_CASE("multicast_discovery: a foreign-universe goodbye fires no on_withdrawn", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for(universe_a)};

    int withdrawn = 0;
    disc.browse([&](const plexus::discovery::service_info &) {});
    disc.on_withdrawn([&](const plexus::discovery::service_info &) { ++withdrawn; });
    socket.inject("10.0.0.9", inbound_bytes(0x23, universe_b, plexus::wire::k_announcement_goodbye_flag));

    REQUIRE(withdrawn == 0);
}

TEST_CASE("multicast_discovery: a same-universe announcement resolves", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for(universe_a)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    socket.inject("172.16.0.4", inbound_bytes(0x24, universe_a));

    REQUIRE(resolved == 1);
}

TEST_CASE("multicast_discovery: two configless leaves rendezvous on the default universe", "[multicast_discovery][universe]")
{
    int ex_b = 0;
    fake_datagram_socket socket_b;
    discovery_under_test node_b{ex_b, socket_b};
    node_b.advertise({"node", plexus::io::endpoint{"", "host"}, plexus::discovery::assemble_contact_card(make_id(0x31), sample_listens())});
    REQUIRE(socket_b.sent.size() == 1);

    int ex_a = 0;
    fake_datagram_socket socket_a;
    discovery_under_test node_a{ex_a, socket_a};

    int resolved = 0;
    node_a.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    socket_a.inject("192.168.1.31", socket_b.sent.front());

    REQUIRE(resolved == 1);
}

TEST_CASE("multicast_discovery: a self-echo latches the self-probe under a custom universe", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for(universe_a)};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    disc.advertise({"node", plexus::io::endpoint{"", "host"}, plexus::discovery::assemble_contact_card(make_id(0x41), sample_listens())});
    REQUIRE(socket.sent.size() == 1);

    socket.inject("192.168.1.41", socket.sent.front());

    REQUIRE(resolved == 0);
    REQUIRE(disc.probe(std::chrono::steady_clock::now(), std::chrono::milliseconds{1000}) == plexus::discovery::discovery_health::healthy);
}
