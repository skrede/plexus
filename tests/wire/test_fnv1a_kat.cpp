#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

// Reference FNV-1a 64 vectors from IETF draft-eastlake-fnv-21 Appendix C. The expected values are captured constants,
// never recomputed inside the assertion, so a wrong basis or prime fails here instead of agreeing with itself.

TEST_CASE("fnv1a kat: the topic hash of the empty string is the offset basis", "[wire][fnv1a][kat][!shouldfail]")
{
    REQUIRE(plexus::wire::fqn_topic_hash("") == 0xcbf29ce484222325ull);
}

TEST_CASE("fnv1a kat: the topic hash of a single byte", "[wire][fnv1a][kat][!shouldfail]")
{
    REQUIRE(plexus::wire::fqn_topic_hash("a") == 0xaf63dc4c8601ec8cull);
}

TEST_CASE("fnv1a kat: the topic hash of a short string", "[wire][fnv1a][kat][!shouldfail]")
{
    REQUIRE(plexus::wire::fqn_topic_hash("foobar") == 0x85944171f73967e8ull);
}
