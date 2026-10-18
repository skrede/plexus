#include "plexus/detail/node_identity.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/endpoint_id.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

// Reference FNV-1a 64 vectors from IETF draft-eastlake-fnv-21 Appendix C for the low lane of a node id. An endpoint id
// hashes the scheme, a zero byte and the address, so its vectors are the reference function over those bytes. The
// expected values are captured constants, never recomputed inside the assertion.

namespace {

std::uint64_t low_lane(const plexus::node_id &id)
{
    std::uint64_t lo = 0;
    for(std::size_t i = 0; i < 8; ++i)
        lo |= static_cast<std::uint64_t>(id[i]) << (8 * i);
    return lo;
}

std::uint64_t node_low(std::string_view name)
{
    return low_lane(plexus::node_id_from_name(name));
}

std::uint64_t endpoint_low(std::string_view scheme, std::string_view address)
{
    return low_lane(plexus::io::endpoint_id(plexus::io::endpoint{std::string{scheme}, std::string{address}}));
}

}

TEST_CASE("fnv1a kat: the node id low lane of the empty name is the offset basis", "[node][fnv1a][kat][!shouldfail]")
{
    REQUIRE(node_low("") == 0xcbf29ce484222325ull);
}

TEST_CASE("fnv1a kat: the node id low lane of a single byte", "[node][fnv1a][kat][!shouldfail]")
{
    REQUIRE(node_low("a") == 0xaf63dc4c8601ec8cull);
}

TEST_CASE("fnv1a kat: the node id low lane of a short string", "[node][fnv1a][kat][!shouldfail]")
{
    REQUIRE(node_low("foobar") == 0x85944171f73967e8ull);
}

TEST_CASE("fnv1a kat: the endpoint id low lane of an empty endpoint", "[node][fnv1a][kat][!shouldfail]")
{
    REQUIRE(endpoint_low("", "") == 0xaf63bd4c8601b7dfull);
}

TEST_CASE("fnv1a kat: the endpoint id low lane of a scheme with no address", "[node][fnv1a][kat][!shouldfail]")
{
    REQUIRE(endpoint_low("foobar", "") == 0x34531ca7168b8f38ull);
}

TEST_CASE("fnv1a kat: the endpoint id low lane of a scheme and an address", "[node][fnv1a][kat][!shouldfail]")
{
    REQUIRE(endpoint_low("foo", "bar") == 0x8a5f40d8243888aaull);
}
