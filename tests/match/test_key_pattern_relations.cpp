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

// Does a's keyset contain b's (language containment L(b) subset L(a))? The same
// constexpr no-heap / bounded-evaluation evidence as rel above.
constexpr bool inc(std::string_view a, std::string_view b) noexcept
{
    return key_pattern::make(a)->includes(*key_pattern::make(b));
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

// INCLUDES (a superset of b) — the pinned containment truth table.
static_assert(inc("**", "a") && inc("**", "a/b") && inc("**", "a/b/c"));
static_assert(inc("**", "**") && inc("**", "a/*") && inc("**", "a/**"));
static_assert(inc("a/**", "a/b") && inc("a/**", "a/*") && inc("a/**", "a/**") && inc("a/**", "a/b/c"));
static_assert(!inc("a/**", "b/x") && !inc("a/**", "x/a/b"));
static_assert(inc("a/*", "a/b") && inc("a/*", "a/*"));
static_assert(!inc("a/*", "a/**") && !inc("a/*", "a/b/c"));
static_assert(inc("*", "a") && inc("*", "*"));
static_assert(!inc("*", "**") && !inc("*", "a/b"));

// includes is reflexive and not generally symmetric.
static_assert(inc("a/**/c", "a/**/c") && inc("a/*/c", "a/*/c") && inc("**", "**"));
static_assert(inc("a/**", "a/b") && !inc("a/b", "a/**"));

// includes ⇒ intersects on the covered pairs (the cross-relation invariant).
static_assert(!inc("a/**", "a/b/c") || rel("a/**", "a/b/c"));
static_assert(!inc("**", "a/*") || rel("**", "a/*"));
static_assert(!inc("a/*", "a/b") || rel("a/*", "a/b"));

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

TEST_CASE("key_pattern includes pins the containment truth table", "[match][key_pattern][includes]")
{
    REQUIRE(inc("**", "a"));
    REQUIRE(inc("**", "a/b/c"));
    REQUIRE(inc("**", "a/**"));
    REQUIRE(inc("a/**", "a/b"));
    REQUIRE(inc("a/**", "a/*"));
    REQUIRE(inc("a/**", "a/b/c"));
    REQUIRE_FALSE(inc("a/**", "b/x"));
    REQUIRE_FALSE(inc("a/**", "x/a/b"));
    REQUIRE(inc("a/*", "a/b"));
    REQUIRE(inc("a/*", "a/*"));
    REQUIRE_FALSE(inc("a/*", "a/**"));
    REQUIRE_FALSE(inc("a/*", "a/b/c"));
    REQUIRE(inc("*", "a"));
    REQUIRE_FALSE(inc("*", "**"));
    REQUIRE_FALSE(inc("*", "a/b"));
}

TEST_CASE("key_pattern includes is reflexive and not generally symmetric", "[match][key_pattern][includes]")
{
    constexpr std::array<std::string_view, 8> patterns{
            "a", "*", "**", "a/*", "a/**", "**/c", "a/**/c", "a/*/c"};
    for(const std::string_view p : patterns)
        REQUIRE(inc(p, p));
    REQUIRE(inc("a/**", "a/b"));
    REQUIRE_FALSE(inc("a/b", "a/**"));
}

TEST_CASE("key_pattern includes implies intersects on every covered pair", "[match][key_pattern][includes]")
{
    constexpr std::array<std::string_view, 8> patterns{
            "a", "*", "**", "a/*", "a/**", "**/c", "a/**/c", "a/*/c"};
    for(const std::string_view p : patterns)
        for(const std::string_view q : patterns)
            if(inc(p, q))
                REQUIRE(rel(p, q));
}

TEST_CASE("key_pattern relations treat **/** as the single canonical ** form", "[match][key_pattern][includes]")
{
    REQUIRE_FALSE(key_pattern::make("**/**").has_value());
    REQUIRE(key_pattern::make("**/**").error() == plexus::match::key_pattern_error::non_canonical);
    const auto star = key_pattern::make("**");
    REQUIRE(star.has_value());
    REQUIRE(star->includes(*star));
    REQUIRE(star->intersects(*star));
}
