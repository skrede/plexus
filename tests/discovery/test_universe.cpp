#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/universe.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>

using namespace plexus::discovery;

TEST_CASE("universe_from_label pins the well-known default hash", "[discovery][universe]")
{
    STATIC_REQUIRE(k_default_universe == 0x58E1B347u);
    REQUIRE(universe_from_label("plexus.default") == k_default_universe);
    // A second known-answer label: the hash is neither the identity nor a constant.
    REQUIRE(universe_from_label("warehouse-fleet") == 0x98A637CDu);
    REQUIRE(universe_from_label("warehouse-fleet") != universe_from_label("plexus.default"));
}

TEST_CASE("universe group derivation avoids the reserved multicast groups", "[discovery][universe]")
{
    for(std::uint32_t u = 0; u < 70000u; ++u)
    {
        const auto [x, y] = universe_group_octets(u);
        REQUIRE(x >= 1u);
        REQUIRE(x <= 254u);
        REQUIRE(y <= 255u);
    }
    for(std::uint32_t u : {0u, 0xFFFFFFFFu, k_default_universe})
    {
        const auto [x, y] = universe_group_octets(u);
        REQUIRE(x >= 1u);
        REQUIRE(x <= 254u);
        const std::string g = universe_group(u);
        REQUIRE(g != "239.255.0.1");
        REQUIRE(g != "239.255.0.7");
        REQUIRE(g != "239.255.255.250");
        REQUIRE(g != "239.255.255.253");
    }
    REQUIRE(universe_group(k_default_universe) == "239.255.115.113");
}

TEST_CASE("effective_group selects the universe group per the scoping mode", "[discovery][universe]")
{
    discovery_options opts;
    REQUIRE(opts.universe == k_default_universe);
    REQUIRE(opts.scoping == universe_scoping::soft);
    REQUIRE(effective_group(opts) == opts.group);

    opts.scoping = universe_scoping::hard;
    REQUIRE(effective_group(opts) == universe_group(opts.universe));

    opts.group = "239.255.42.42";
    REQUIRE(effective_group(opts) == universe_group(opts.universe));

    opts.universe = universe_from_label("warehouse-fleet");
    REQUIRE(effective_group(opts) == universe_group(universe_from_label("warehouse-fleet")));
}
