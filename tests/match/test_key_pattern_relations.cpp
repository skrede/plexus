#include "plexus/match/key_pattern.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string_view>

using plexus::match::key_pattern;

namespace {

// A common concrete key exists in both keysets. When the right operand is a
// concrete key this is exactly "pattern matches key". A constant-expression
// cannot heap-allocate or recurse unboundedly, so every constexpr row below is
// also the compile-time no-heap / bounded-evaluation evidence.
constexpr bool rel(std::string_view a, std::string_view b) noexcept
{
    return key_pattern::make(a)->intersects(*key_pattern::make(b));
}

}

// MATCH (concrete key vs pattern) — the pinned empty-segment truth table.
static_assert(rel("a/**/c", "a/c"));
static_assert(rel("a/**/c", "a/b/c"));
static_assert(rel("a/**/c", "a/b/d/c"));
static_assert(!rel("a/**/c", "a"));
static_assert(!rel("a/**/c", "c"));
static_assert(!rel("a/**/c", "a/c/d"));
static_assert(!rel("a/**/c", "x/c"));
static_assert(rel("**", "a") && rel("**", "a/b") && rel("**", "a/b/c"));
static_assert(rel("a/**", "a") && rel("a/**", "a/b") && rel("a/**", "a/b/c"));
static_assert(rel("**/a", "a") && rel("**/a", "x/a") && rel("**/a", "x/y/a"));
static_assert(rel("*", "a") && rel("*", "x") && !rel("*", "a/b"));
static_assert(rel("a/*/c", "a/b/c") && !rel("a/*/c", "a/c") && !rel("a/*/c", "a/b/d/c"));

// INTERSECTS (symmetric, both operands may carry wildcards).
static_assert(rel("a/*", "a/b"));
static_assert(rel("a/*", "*/b"));
static_assert(rel("a/**", "**/c"));
static_assert(!rel("a/*", "b/*"));
static_assert(!rel("a/*", "a/b/c"));
static_assert(rel("a/**", "a/b/c"));

// Symmetry and reflexivity as compile-time facts.
static_assert(rel("a/*", "*/b") == rel("*/b", "a/*"));
static_assert(rel("a/**", "**/c") == rel("**/c", "a/**"));
static_assert(rel("a/**/c", "a/**/c"));
static_assert(rel("*", "*") && rel("**", "**") && rel("a/*/c", "a/*/c"));

TEST_CASE("key_pattern intersects pins the empty-** match truth table", "[match][key_pattern][intersects]")
{
    REQUIRE(rel("a/**/c", "a/c"));
    REQUIRE(rel("a/**/c", "a/b/c"));
    REQUIRE(rel("a/**/c", "a/b/d/c"));
    REQUIRE_FALSE(rel("a/**/c", "a"));
    REQUIRE_FALSE(rel("a/**/c", "c"));
    REQUIRE_FALSE(rel("a/**/c", "a/c/d"));
    REQUIRE_FALSE(rel("a/**/c", "x/c"));
}

TEST_CASE("key_pattern intersects pins the single-segment star truth table", "[match][key_pattern][intersects]")
{
    REQUIRE(rel("**", "a/b/c"));
    REQUIRE(rel("a/**", "a"));
    REQUIRE(rel("**/a", "x/y/a"));
    REQUIRE(rel("*", "a"));
    REQUIRE_FALSE(rel("*", "a/b"));
    REQUIRE(rel("a/*/c", "a/b/c"));
    REQUIRE_FALSE(rel("a/*/c", "a/c"));
    REQUIRE_FALSE(rel("a/*/c", "a/b/d/c"));
}

TEST_CASE("key_pattern intersects pins the pattern-vs-pattern truth table", "[match][key_pattern][intersects]")
{
    REQUIRE(rel("a/*", "a/b"));
    REQUIRE(rel("a/*", "*/b"));
    REQUIRE(rel("a/**", "**/c"));
    REQUIRE_FALSE(rel("a/*", "b/*"));
    REQUIRE_FALSE(rel("a/*", "a/b/c"));
    REQUIRE(rel("a/**", "a/b/c"));
}

TEST_CASE("key_pattern intersects is symmetric and reflexive", "[match][key_pattern][intersects]")
{
    constexpr std::array<std::string_view, 8> patterns{
            "a", "*", "**", "a/*", "a/**", "**/c", "a/**/c", "a/*/c"};
    for(const std::string_view p : patterns)
    {
        REQUIRE(rel(p, p));
        for(const std::string_view q : patterns)
            REQUIRE(rel(p, q) == rel(q, p));
    }
}

TEST_CASE("key_pattern intersects stays linear on adversarial star runs", "[match][key_pattern][intersects]")
{
    const auto pattern = key_pattern::make("a/*/a/*/a/*/a/*/a");
    const auto key     = key_pattern::make("a/a/a/a/a/a/a/a/b");
    REQUIRE(pattern.has_value());
    REQUIRE(key.has_value());
    REQUIRE_FALSE(pattern->intersects(*key));
}
