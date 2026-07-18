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
#include <string_view>

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

// A peer announcement carrying a universe pattern: announcement_from_card sets the presence flag itself
// (the label is non-default), and the uint32 is derived so a concrete peer also rendezvous on the
// fast-path. The pattern bytes are carried verbatim — even a malformed pattern, which the receiver
// rejects at make() (value-agnostic wire).
std::vector<std::byte> inbound_pattern_bytes(std::uint8_t seed, std::string_view pattern)
{
    return plexus::wire::encode_announcement(
            plexus::discovery::detail::announcement_from_card(make_id(seed), sample_listens(), 30, 0, plexus::discovery::universe_from_label(pattern), pattern));
}

plexus::discovery::discovery_options options_for(std::uint32_t universe)
{
    plexus::discovery::discovery_options opts;
    opts.universe = universe;
    return opts;
}

plexus::discovery::discovery_options options_for_pattern(std::string_view pattern)
{
    plexus::discovery::discovery_options opts;
    opts.universe_pattern = std::string(pattern);
    return opts; // the multicast_discovery ctor derives the uint32 from the label
}

plexus::discovery::service_info card_for(std::uint8_t seed)
{
    return {"node", plexus::io::endpoint{"", "host"}, plexus::discovery::assemble_contact_card(make_id(seed), sample_listens())};
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

TEST_CASE("multicast_discovery: a wildcard universe resolves a concrete peer it covers", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for_pattern("factory/line/*")};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    socket.inject("10.0.0.1", inbound_pattern_bytes(0x51, "factory/line/1"));

    REQUIRE(resolved == 1);
}

TEST_CASE("multicast_discovery: a wildcard universe drops a concrete peer it does not cover", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for_pattern("factory/line/*")};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    socket.inject("10.0.0.2", inbound_pattern_bytes(0x52, "warehouse/line/1"));

    REQUIRE(resolved == 0);
}

TEST_CASE("multicast_discovery: concrete rendezvous is unchanged via the uint32 fast-path", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for_pattern("depot")};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    // A flagless (legacy uint32-only) peer of the same derived universe rides the fast-path.
    socket.inject("10.0.0.3", inbound_bytes(0x53, plexus::discovery::universe_from_label("depot")));
    REQUIRE(resolved == 1);
    // A flagless peer of a different universe is dropped on the same fast-path.
    socket.inject("10.0.0.4", inbound_bytes(0x54, plexus::discovery::universe_from_label("annex")));
    REQUIRE(resolved == 1);
}

TEST_CASE("multicast_discovery: the default universe never matches a foreign concrete universe", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket}; // configless: the concrete default label, never match-all

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    // A pattern-flagged peer on a foreign concrete universe: the default is a literal that intersects
    // only itself, so it must NOT wildcard-match the peer.
    socket.inject("10.0.0.5", inbound_pattern_bytes(0x55, "alpha"));
    REQUIRE(resolved == 0);
    // And a flagless foreign peer is dropped on the uint32 fast-path.
    socket.inject("10.0.0.6", inbound_bytes(0x56, plexus::discovery::universe_from_label("beta")));
    REQUIRE(resolved == 0);
}

TEST_CASE("multicast_discovery: a malformed peer universe pattern fails closed", "[multicast_discovery][universe]")
{
    int ex = 0;
    fake_datagram_socket socket;
    discovery_under_test disc{ex, socket, options_for_pattern("factory/line/*")};

    int resolved = 0;
    disc.browse([&](const plexus::discovery::service_info &) { ++resolved; });
    // A stray '*' inside a segment is not a whole-segment wildcard: make() rejects it, so the reject
    // site drops the datagram before any admission.
    socket.inject("10.0.0.7", inbound_pattern_bytes(0x57, "factory/li*ne"));

    REQUIRE(resolved == 0);
}

TEST_CASE("multicast_discovery: a default node's real stamped announcement carries no pattern flag", "[multicast_discovery][universe]")
{
    int ex_default = 0;
    fake_datagram_socket socket_default;
    discovery_under_test node_default{ex_default, socket_default};
    node_default.advertise(card_for(0x61));
    REQUIRE(socket_default.sent.size() == 1);
    const auto default_ann = plexus::wire::decode_announcement(socket_default.sent.front());
    REQUIRE(default_ann.has_value());
    REQUIRE((default_ann->flags & plexus::wire::k_announcement_universe_pattern_flag) == 0);
    REQUIRE(default_ann->universe_pattern.empty());

    // The legacy uint32-only shape (default label, custom uint32) also stamps flagless — never a
    // flagged stale default.
    int ex_legacy = 0;
    fake_datagram_socket socket_legacy;
    discovery_under_test node_legacy{ex_legacy, socket_legacy, options_for(universe_a)};
    node_legacy.advertise(card_for(0x62));
    REQUIRE(socket_legacy.sent.size() == 1);
    const auto legacy_ann = plexus::wire::decode_announcement(socket_legacy.sent.front());
    REQUIRE(legacy_ann.has_value());
    REQUIRE((legacy_ann->flags & plexus::wire::k_announcement_universe_pattern_flag) == 0);
    REQUIRE(legacy_ann->universe_pattern.empty());
}

TEST_CASE("multicast_discovery: two different concrete universes do not mutually admit through the emit loop", "[multicast_discovery][universe]")
{
    int ex_alpha = 0;
    fake_datagram_socket socket_alpha;
    discovery_under_test node_alpha{ex_alpha, socket_alpha, options_for_pattern("alpha")};
    node_alpha.advertise(card_for(0x63));
    REQUIRE(socket_alpha.sent.size() == 1);

    int ex_beta = 0;
    fake_datagram_socket socket_beta;
    discovery_under_test node_beta{ex_beta, socket_beta, options_for_pattern("beta")};
    node_beta.advertise(card_for(0x64));
    REQUIRE(socket_beta.sent.size() == 1);

    // The real stamp path sets the presence flag and the pattern bytes from the label.
    const auto alpha_ann = plexus::wire::decode_announcement(socket_alpha.sent.front());
    REQUIRE(alpha_ann.has_value());
    REQUIRE((alpha_ann->flags & plexus::wire::k_announcement_universe_pattern_flag) != 0);
    REQUIRE(alpha_ann->universe_pattern == "alpha");

    int resolved_alpha = 0;
    int resolved_beta  = 0;
    node_alpha.browse([&](const plexus::discovery::service_info &) { ++resolved_alpha; });
    node_beta.browse([&](const plexus::discovery::service_info &) { ++resolved_beta; });

    // Feed each node's OWN stamped announcement into the other — no mutual admission in either direction.
    socket_alpha.inject("10.0.0.64", socket_beta.sent.front());
    socket_beta.inject("10.0.0.63", socket_alpha.sent.front());

    REQUIRE(resolved_alpha == 0);
    REQUIRE(resolved_beta == 0);
}
