// The single source compiled by both the host build and the ESP-IDF xtensa cross-compile:
// the byte-identical INV-1 proof that the header-only matcher is pure logic with no platform
// #ifdef. make(), intersects(), and includes() are exercised in a constexpr context, so the
// static_asserts firing identically on both targets is the byte-identical-semantics evidence;
// a trivial runtime entry links the host leg.

#include "plexus/match/key_pattern.h"

namespace
{

using plexus::match::key_pattern;

constexpr bool star_covers_literal()
{
    const auto star = key_pattern::make("*");
    const auto leaf = key_pattern::make("a");
    return star && leaf && star->includes(*leaf) && star->intersects(*leaf);
}

constexpr bool deep_pattern_reflexive()
{
    const auto pattern = key_pattern::make("a/**/c");
    return pattern && pattern->intersects(*pattern) && pattern->includes(*pattern);
}

constexpr bool disjoint_literals_never_meet()
{
    const auto lhs = key_pattern::make("a");
    const auto rhs = key_pattern::make("b");
    return lhs && rhs && !lhs->intersects(*rhs) && !lhs->includes(*rhs);
}

static_assert(star_covers_literal());
static_assert(deep_pattern_reflexive());
static_assert(disjoint_literals_never_meet());

}

int main()
{
    return (star_covers_literal() && deep_pattern_reflexive() && disjoint_literals_never_meet())
                   ? 0
                   : 1;
}
