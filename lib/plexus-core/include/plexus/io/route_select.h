#ifndef HPP_GUARD_PLEXUS_IO_ROUTE_SELECT_H
#define HPP_GUARD_PLEXUS_IO_ROUTE_SELECT_H

#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include <span>
#include <cstddef>

namespace plexus::io
{

// The empty-span result of route_select: no candidate to address.
inline constexpr std::size_t route_select_npos = static_cast<std::size_t>(-1);

// Strict preference between two candidates: a direct candidate outranks any relayed one, then a
// reachable candidate outranks an unreachable one, then fewer hops outrank more. The reachability
// gate keeps a row whose via has degraded (its relay died) from winning over a live path to the same
// origin; a direct row is always reachable, so the direct-only ranking is unperturbed. Equal rank is
// not a strict win, so a scanning caller keeps the first-in-span candidate as the deterministic
// tie-break.
constexpr bool route_outranks(const route_candidate &a, const route_candidate &b) noexcept
{
    if(a.is_direct() != b.is_direct())
        return a.is_direct();
    const bool a_reachable = a.origin.reach_status == graph::reachability::reachable;
    const bool b_reachable = b.origin.reach_status == graph::reachability::reachable;
    if(a_reachable != b_reachable)
        return a_reachable;
    return a.hop < b.hop;
}

// Pick the preferred route over a candidate span, returning its index (route_select_npos when the
// span is empty). Ranking: direct before relayed, then fewer hops before more, with first-in-span
// order as the deterministic tie-break — so the choice is reproducible for a fixed span order (equal
// -rank candidates resolve to the earliest, which does depend on how the span is laid out). Pure: no
// state, no allocation, no I/O.
inline std::size_t route_select(std::span<const route_candidate> candidates) noexcept
{
    std::size_t best = route_select_npos;
    for(std::size_t i = 0; i < candidates.size(); ++i)
        if(best == route_select_npos || route_outranks(candidates[i], candidates[best]))
            best = i;
    return best;
}

// The route_usage-aware pick. never rejects a relayed winner (a relayed candidate is not a usable
// route, so a span holding only relayed candidates yields npos), while prefer_direct and allow_relayed
// return route_select's rank unchanged — both differ from prefer_direct only in whether the caller
// falls through to a live relayed session, which is a session-liveness decision the pure pick cannot
// see. Because route_select already ranks a direct candidate ahead of any relayed one, a span holding
// any direct candidate is unaffected by the mode. Pure: no state, no allocation, no I/O.
inline std::size_t route_select(std::span<const route_candidate> candidates, route_usage usage) noexcept
{
    const std::size_t best = route_select(candidates);
    if(usage == route_usage::never && best != route_select_npos && !candidates[best].is_direct())
        return route_select_npos;
    return best;
}

}

#endif
