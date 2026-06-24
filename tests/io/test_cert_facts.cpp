// The cert_facts oracle: a pure sans-OpenSSL drive of the peer-cert VALUE struct and the
// identity stable-subset derivation (no X509, no crypto, no backend link — plexus::plexus
// only). It proves the value contract the verify decision relies on: the full field list
// round-trips, the identity subset (spki_sha256 + subject + san) is readable, and the
// node_id derivation equals the first 16 bytes of spki_sha256.

#include "plexus/io/security/cert_facts.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <vector>
#include <cstddef>

using plexus::io::security::cert_facts;
using plexus::io::security::to_node_id;

namespace {

std::array<std::byte, 32> digest_of(int seed)
{
    std::array<std::byte, 32> d{};
    for(std::size_t i = 0; i < d.size(); ++i)
        d[i] = static_cast<std::byte>((seed + static_cast<int>(i)) & 0xff);
    return d;
}

}

TEST_CASE("io.cert_facts carries the full field list and round-trips the identity subset", "[io][cert_facts]")
{
    cert_facts facts;
    facts.spki_sha256  = digest_of(1);
    facts.subject      = "CN=peer.example";
    facts.san          = {"peer.example", "192.0.2.1"};
    facts.chain_depth  = 0;
    facts.preverify_ok = true;

    // The identity stable subset is readable as stored.
    REQUIRE(facts.spki_sha256 == digest_of(1));
    REQUIRE(facts.subject == "CN=peer.example");
    REQUIRE(facts.san.size() == 2);
    REQUIRE(facts.san[0] == "peer.example");

    // The remaining D-list fields exist with their documented defaults / values.
    REQUIRE(facts.chain_depth == 0);
    REQUIRE(facts.preverify_ok == true);
    REQUIRE(facts.not_before == facts.not_after); // both value-initialized here
}

TEST_CASE("io.cert_facts node_id is the first 16 bytes of spki_sha256", "[io][cert_facts]")
{
    cert_facts facts;
    facts.spki_sha256 = digest_of(7);

    const auto id = to_node_id(facts);
    REQUIRE(id.size() == 16);
    for(std::size_t i = 0; i < id.size(); ++i)
        REQUIRE(id[i] == facts.spki_sha256[i]);
}
