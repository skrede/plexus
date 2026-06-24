#include "test_contact_card_common.h"

using namespace contact_card_fixture;

TEST_CASE("contact_card carries exactly node_id, per-transport port keys, and the schema", "[discovery][contact_card]")
{
    const auto id   = node_id_of(1);
    const auto card = assemble_contact_card(id, {{"tcp", 5000}, {"udp", 5001}});

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

TEST_CASE("contact_card never carries topic, type, publisher, posture, or key-id data", "[discovery][contact_card]")
{
    const auto card = assemble_contact_card(node_id_of(2), {{"tcp", 6000}});

    REQUIRE_FALSE(has_key(card, "type_id"));
    REQUIRE_FALSE(has_key(card, "publisher_gid"));
    REQUIRE_FALSE(has_key(card, "topic"));
    REQUIRE_FALSE(has_key(card, "posture"));
    REQUIRE_FALSE(has_key(card, "key_id"));
}

TEST_CASE("contact_card node_id is the authenticated-peer identity, not a self-asserted value", "[discovery][contact_card]")
{
    // The advertised node_id comes from the authenticated attach binding (the id that
    // produced the validated proof), read via the host-identity accessor — never a
    // self-chosen field on the wire/TXT.
    plexus::io::security::attach_facts facts;
    facts.initiator_id = node_id_of(10);
    facts.responder_id = node_id_of(20);
    facts.role         = plexus::io::security::attach_role::initiator;

    const auto authed_peer = plexus::io::authenticated_peer_id(facts);
    REQUIRE(authed_peer == node_id_of(20));

    const auto card = assemble_contact_card(authed_peer, {{"tcp", 7000}});
    REQUIRE(value_of(card, "node_id") == plexus::discovery::detail::hex_encode(node_id_of(20)));
}

TEST_CASE("contact_card metadata carries verbatim through static_discovery", "[discovery][contact_card]")
{
    service_info advertised;
    advertised.name     = "node-a";
    advertised.endpoint = {"tcp", "192.0.2.10:5000"};
    advertised.metadata = assemble_contact_card(node_id_of(3), {{"tcp", 5000}, {"udp", 5001}});

    static_discovery disco{{}};
    disco.advertise(advertised);

    std::vector<service_info> resolved;
    disco.browse([&](const service_info &svc) { resolved.push_back(svc); });

    REQUIRE(resolved.size() == 1);
    REQUIRE(resolved.front().metadata == advertised.metadata);
}

TEST_CASE("contact_card lets a browsing peer derive its dial port with no hardcoded port", "[discovery][contact_card]")
{
    service_info advertised;
    advertised.name     = "node-b";
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
    const std::string host        = "192.0.2.11";
    const std::string dial_target = host + ":" + std::to_string(*tcp_port);
    REQUIRE(dial_target == "192.0.2.11:5500");
    REQUIRE_FALSE(read_transport_port(found.metadata, "serial").has_value());
}

TEST_CASE("hex_decode is the exact inverse of hex_encode for arbitrary node ids", "[discovery][contact_card]")
{
    // Property-style round-trip: decode(encode(id)) == id over a spread of ids.
    for(int seed = 0; seed < 256; ++seed)
    {
        const auto id      = node_id_of(seed);
        const auto decoded = hex_decode(hex_encode(id));
        REQUIRE(decoded.has_value());
        REQUIRE(*decoded == id);
    }
}

TEST_CASE("hex_decode rejects everything but exactly 32 lowercase hex characters", "[discovery][contact_card]")
{
    // A valid 32-lower-hex baseline that decodes.
    const std::string valid = hex_encode(node_id_of(7));
    REQUIRE(valid.size() == 32);
    REQUIRE(hex_decode(valid).has_value());

    // Reject table: wrong length, uppercase, a non-hex letter, empty, and an embedded NUL.
    REQUIRE_FALSE(hex_decode(valid.substr(0, 31)).has_value());                // 31 chars
    REQUIRE_FALSE(hex_decode(valid + "0").has_value());                        // 33 chars
    REQUIRE_FALSE(hex_decode(std::string(32, 'A')).has_value());               // uppercase
    REQUIRE_FALSE(hex_decode("0123456789abcdef0123456789abcdeg").has_value()); // 'g'
    REQUIRE_FALSE(hex_decode("").has_value());                                 // empty

    std::string with_nul(32, '0');
    with_nul[10] = '\0'; // embedded NUL (length stays 32)
    REQUIRE(with_nul.size() == 32);
    REQUIRE_FALSE(hex_decode(with_nul).has_value());

    // A mixed-case otherwise-valid string is rejected (no uppercase tolerated).
    std::string mixed = valid;
    mixed[0]          = static_cast<char>(std::toupper(static_cast<unsigned char>(mixed[0])));
    if(mixed != valid) // only meaningful when the first nibble was a letter
        REQUIRE_FALSE(hex_decode(mixed).has_value());
}
