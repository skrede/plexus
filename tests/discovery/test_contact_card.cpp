// The contact-card oracle: the node-level discovery card is node_id + one port key
// per listening transport + a schema version, and NOTHING else. It proves the exact
// key set, the structural absence of any topic/type/publisher/posture/key-id key, the
// verbatim carry through static_discovery, that the advertised node_id is the
// authenticated-peer identity (not a self-asserted value), and that a browsing peer
// derives its dial port from the card with no hardcoded port. Header-only core only
// (plexus::plexus) — no backend, no socket, no mDNS.

#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/static_discovery.h"
#include "plexus/io/host_identity.h"
#include "plexus/io/security/attach_facts.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <algorithm>

using plexus::discovery::assemble_contact_card;
using plexus::discovery::read_transport_port;
using plexus::discovery::listening_transport;
using plexus::discovery::service_info;
using plexus::discovery::static_discovery;

namespace {

plexus::node_id node_id_of(int seed)
{
    plexus::node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>((seed + static_cast<int>(i)) & 0xff);
    return id;
}

bool has_key(const std::vector<std::pair<std::string, std::string>> &card, const std::string &key)
{
    return std::ranges::any_of(card, [&](const auto &kv) { return kv.first == key; });
}

std::string value_of(const std::vector<std::pair<std::string, std::string>> &card, const std::string &key)
{
    for(const auto &[k, v] : card)
        if(k == key)
            return v;
    return {};
}

}

TEST_CASE("contact_card carries exactly node_id, per-transport port keys, and the schema",
          "[discovery][contact_card]")
{
    const auto id = node_id_of(1);
    const auto card = assemble_contact_card(
        id, {{"tcp", 5000}, {"udp", 5001}});

    REQUIRE(card.size() == 4);
    REQUIRE(has_key(card, "node_id"));
    REQUIRE(has_key(card, "plexus/tcp/port"));
    REQUIRE(has_key(card, "plexus/udp/port"));
    REQUIRE(has_key(card, "plexus/schema"));

    REQUIRE(value_of(card, "plexus/tcp/port") == "5000");
    REQUIRE(value_of(card, "plexus/udp/port") == "5001");
    REQUIRE(value_of(card, "plexus/schema") == "1");
    REQUIRE(value_of(card, "node_id") == plexus::discovery::detail::hex_encode(id));
}

TEST_CASE("contact_card never carries topic, type, publisher, posture, or key-id data",
          "[discovery][contact_card]")
{
    const auto card = assemble_contact_card(node_id_of(2), {{"tcp", 6000}});

    REQUIRE_FALSE(has_key(card, "type_id"));
    REQUIRE_FALSE(has_key(card, "publisher_gid"));
    REQUIRE_FALSE(has_key(card, "topic"));
    REQUIRE_FALSE(has_key(card, "posture"));
    REQUIRE_FALSE(has_key(card, "key_id"));
}

TEST_CASE("contact_card node_id is the authenticated-peer identity, not a self-asserted value",
          "[discovery][contact_card]")
{
    // The advertised node_id comes from the authenticated attach binding (the id that
    // produced the validated proof), read via the host-identity accessor — never a
    // self-chosen field on the wire/TXT.
    plexus::io::security::attach_facts facts;
    facts.initiator_id = node_id_of(10);
    facts.responder_id = node_id_of(20);
    facts.role = plexus::io::security::attach_role::initiator;

    const auto authed_peer = plexus::io::authenticated_peer_id(facts);
    REQUIRE(authed_peer == node_id_of(20));

    const auto card = assemble_contact_card(authed_peer, {{"tcp", 7000}});
    REQUIRE(value_of(card, "node_id") == plexus::discovery::detail::hex_encode(node_id_of(20)));
}

TEST_CASE("contact_card metadata carries verbatim through static_discovery",
          "[discovery][contact_card]")
{
    service_info advertised;
    advertised.name = "node-a";
    advertised.endpoint = {"tcp", "192.0.2.10:5000"};
    advertised.metadata = assemble_contact_card(node_id_of(3), {{"tcp", 5000}, {"udp", 5001}});

    static_discovery disco{{}};
    disco.advertise(advertised);

    std::vector<service_info> resolved;
    disco.browse([&](const service_info &svc) { resolved.push_back(svc); });

    REQUIRE(resolved.size() == 1);
    REQUIRE(resolved.front().metadata == advertised.metadata);
}

TEST_CASE("contact_card lets a browsing peer derive its dial port with no hardcoded port",
          "[discovery][contact_card]")
{
    service_info advertised;
    advertised.name = "node-b";
    advertised.endpoint = {"tcp", "192.0.2.11:0"};
    advertised.metadata = assemble_contact_card(node_id_of(4), {{"tcp", 5500}, {"udp", 5501}});

    static_discovery disco{{}};
    disco.advertise(advertised);

    service_info found;
    disco.browse([&](const service_info &svc) { found = svc; });

    const auto tcp_port = read_transport_port(found.metadata, "tcp");
    const auto udp_port = read_transport_port(found.metadata, "udp");
    REQUIRE(tcp_port.has_value());
    REQUIRE(udp_port.has_value());
    REQUIRE(*tcp_port == 5500);
    REQUIRE(*udp_port == 5501);

    // The derived dial target is the resolved IPv4 host joined with the advertised
    // port — nothing hardcoded; an absent key yields no port.
    const std::string host = "192.0.2.11";
    const std::string dial_target = host + ":" + std::to_string(*tcp_port);
    REQUIRE(dial_target == "192.0.2.11:5500");
    REQUIRE_FALSE(read_transport_port(found.metadata, "serial").has_value());
}
