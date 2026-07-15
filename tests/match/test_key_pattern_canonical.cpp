#include "plexus/match/key_pattern.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

using plexus::match::key_pattern;
using plexus::match::key_pattern_error;

namespace {

void expect_error(std::string_view pattern, key_pattern_error expected)
{
    const auto result = key_pattern::make(pattern);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == expected);
}

void expect_ok(std::string_view pattern)
{
    const auto result = key_pattern::make(pattern);
    REQUIRE(result.has_value());
    REQUIRE(result->text() == pattern);
}

std::string deep_pattern(std::size_t segments)
{
    std::string out;
    for(std::size_t i = 0; i < segments; ++i)
        out += (i + 1 == segments) ? "a" : "a/";
    return out;
}

}

// A constant-expression cannot heap-allocate or recurse unboundedly, so a
// constexpr make() is itself the compile-time no-heap / bounded-scan evidence.
static_assert(key_pattern::make("a/**/c").has_value());
static_assert(key_pattern::make("a/b/c").has_value());
static_assert(key_pattern::make("").error() == key_pattern_error::empty);
static_assert(key_pattern::make("**/**").error() == key_pattern_error::non_canonical);

TEST_CASE("key_pattern make refuses empty input", "[match][key_pattern]")
{
    expect_error("", key_pattern_error::empty);
}

TEST_CASE("key_pattern make refuses malformed structure", "[match][key_pattern]")
{
    expect_error("/a", key_pattern_error::malformed);
    expect_error("a/", key_pattern_error::malformed);
    expect_error("a//b", key_pattern_error::malformed);
}

TEST_CASE("key_pattern make refuses partial-segment stars", "[match][key_pattern]")
{
    expect_error("a*b", key_pattern_error::malformed);
    expect_error("*a", key_pattern_error::malformed);
    expect_error("***", key_pattern_error::malformed);
}

TEST_CASE("key_pattern make refuses non-canonical wildcard runs", "[match][key_pattern]")
{
    expect_error("**/**", key_pattern_error::non_canonical);
    expect_error("**/*", key_pattern_error::non_canonical);
}

TEST_CASE("key_pattern make refuses over-long input", "[match][key_pattern]")
{
    expect_error(std::string(300, 'x'), key_pattern_error::too_long);
}

TEST_CASE("key_pattern make refuses over-deep input", "[match][key_pattern]")
{
    expect_ok(deep_pattern(16));
    expect_error(deep_pattern(17), key_pattern_error::too_deep);
    expect_error(deep_pattern(18), key_pattern_error::too_deep);
}

TEST_CASE("key_pattern make accepts canonical patterns", "[match][key_pattern]")
{
    expect_ok("a/b/c");
    expect_ok("*");
    expect_ok("**");
    expect_ok("a/*/c");
    expect_ok("a/**/c");
    expect_ok("a/**");
    expect_ok("**/a");
}

TEST_CASE("key_pattern preserves canonical bytes and segment count", "[match][key_pattern]")
{
    const auto result = key_pattern::make("a/**/c");
    REQUIRE(result.has_value());
    REQUIRE(result->segment_count() == 3);
    REQUIRE(result->segment(0).text == "a");
    REQUIRE(result->segment(1).kind == plexus::match::detail::segment_kind::double_star);
    REQUIRE(result->segment(2).text == "c");
}

TEST_CASE("key_pattern survives copy", "[match][key_pattern]")
{
    const auto result = key_pattern::make("a/**/c");
    REQUIRE(result.has_value());
    const key_pattern copy = *result;
    REQUIRE(copy.text() == "a/**/c");
    REQUIRE(copy.segment_count() == 3);
}
