#include "plexus/match/key_pattern_bounds.h"
#include "plexus/match/key_pattern_error.h"

#include "plexus/wire/subscribe.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>

using plexus::match::default_bounds;
using plexus::match::key_pattern_error;

TEST_CASE("key_pattern default bounds pin the resolved ceilings", "[match][key_pattern]")
{
    STATIC_REQUIRE(default_bounds::k_pattern_depth == 16);
    STATIC_REQUIRE(default_bounds::k_pattern_length == 256);
}

TEST_CASE("key_pattern length ceiling sits below the wire fqn lid", "[match][key_pattern]")
{
    STATIC_REQUIRE(default_bounds::k_pattern_length < plexus::wire::detail::k_max_fqn);
}

TEST_CASE("key_pattern refusal values are mutually distinct", "[match][key_pattern]")
{
    const std::array<key_pattern_error, 5> refusals{
        key_pattern_error::empty,
        key_pattern_error::too_long,
        key_pattern_error::too_deep,
        key_pattern_error::malformed,
        key_pattern_error::non_canonical};

    for(std::size_t i = 0; i < refusals.size(); ++i)
    {
        for(std::size_t j = i + 1; j < refusals.size(); ++j)
        {
            REQUIRE(refusals[i] != refusals[j]);
        }
    }
}
