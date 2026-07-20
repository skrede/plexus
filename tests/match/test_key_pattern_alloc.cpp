#include "plexus/match/key_pattern.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

using plexus::match::key_pattern;

TEST_CASE("key_pattern construction allocates nothing", "[match][key_pattern][alloc]")
{
    // Every input is built BEFORE the snapshot so only make() falls inside the
    // measured window. The adversarial single segment (a*a*a*...) is refused as
    // malformed and the over-long input as too_long — the refusal paths must be
    // as alloc-free as the success paths.
    std::string adversarial;
    for(int i = 0; i < 64; ++i)
        adversarial += "a*";
    adversarial += 'a';
    const std::string over_long(300, 'x');

    const std::string_view literal   = "a/b/c";
    const std::string_view canonical = "a/**/c";
    const std::string_view dstar     = "**";

    const std::size_t before = plexus::testing::alloc_count();
    const auto        a      = key_pattern::make(literal);
    const auto        b      = key_pattern::make(canonical);
    const auto        c      = key_pattern::make(dstar);
    const auto        d      = key_pattern::make(adversarial);
    const auto        e      = key_pattern::make(over_long);
    const std::size_t after  = plexus::testing::alloc_count();

    REQUIRE(a.has_value());
    REQUIRE(b.has_value());
    REQUIRE(c.has_value());
    REQUIRE_FALSE(d.has_value());
    REQUIRE_FALSE(e.has_value());
    REQUIRE(after - before == 0);
}

TEST_CASE("key_pattern intersects allocates nothing", "[match][key_pattern][alloc]")
{
    // The greedy hot path (concrete vs pattern), the rolling-DP path (wildcard
    // pair), and an adversarial deep single-star run must all be alloc-free — the
    // DP row and segment tables are stack-resident std::array.
    const auto concrete   = key_pattern::make("a/b/c/d");
    const auto wildcard   = key_pattern::make("a/**/d");
    const auto other      = key_pattern::make("**/c/d");
    const auto adv_pat    = key_pattern::make("a/*/a/*/a/*/a/*/a");
    const auto adv_key    = key_pattern::make("a/a/a/a/a/a/a/a/a");
    REQUIRE(concrete.has_value());
    REQUIRE(wildcard.has_value());
    REQUIRE(other.has_value());
    REQUIRE(adv_pat.has_value());
    REQUIRE(adv_key.has_value());

    const std::size_t before = plexus::testing::alloc_count();
    const bool        r1     = wildcard->intersects(*concrete);
    const bool        r2     = wildcard->intersects(*other);
    const bool        r3     = adv_pat->intersects(*adv_key);
    const bool        r4     = wildcard->includes(*concrete);
    const bool        r5     = adv_pat->includes(*adv_key);
    const std::size_t after  = plexus::testing::alloc_count();

    REQUIRE(r1);
    REQUIRE(r2);
    REQUIRE(r3);
    REQUIRE(r4);
    REQUIRE(r5);
    REQUIRE(after - before == 0);
}
