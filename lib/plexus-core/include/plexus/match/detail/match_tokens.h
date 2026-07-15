#ifndef HPP_GUARD_PLEXUS_MATCH_DETAIL_MATCH_TOKENS_H
#define HPP_GUARD_PLEXUS_MATCH_DETAIL_MATCH_TOKENS_H

#include "plexus/match/detail/segment_cursor.h"

namespace plexus::match::detail
{

// One pattern segment absorbs exactly one concrete segment: '*' takes any single
// segment, a literal takes only its own bytes. '**' returns false here — the
// greedy kernel routes it through its own zero-or-more star branch.
constexpr bool single_absorbs(segment_view pattern, segment_view key) noexcept
{
    if(pattern.kind == segment_kind::single_star)
        return true;
    if(pattern.kind == segment_kind::double_star)
        return false;
    return pattern.text == key.text;
}

// Two non-'**' tokens can denote a common concrete segment: two literals must be
// byte-equal, and any '*' pairs with anything single.
constexpr bool tokens_compatible(segment_view a, segment_view b) noexcept
{
    if(a.kind == segment_kind::literal && b.kind == segment_kind::literal)
        return a.text == b.text;
    return true;
}

}

#endif
