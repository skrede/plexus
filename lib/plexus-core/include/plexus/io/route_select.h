#ifndef HPP_GUARD_PLEXUS_IO_ROUTE_SELECT_H
#define HPP_GUARD_PLEXUS_IO_ROUTE_SELECT_H

#include "plexus/io/route_candidate.h"

#include <span>
#include <cstddef>

namespace plexus::io
{

// The empty-span result of route_select: no candidate to address.
inline constexpr std::size_t route_select_npos = static_cast<std::size_t>(-1);

// Strict preference between two candidates: a direct candidate outranks any relayed one, then fewer
// hops outrank more. Equal rank is not a strict win, so a scanning caller keeps the first-in-span
// candidate as the deterministic tie-break.
constexpr bool route_outranks(const route_candidate &a, const route_candidate &b) noexcept
{
    if(a.is_direct() != b.is_direct())
        return a.is_direct();
    return a.hop < b.hop;
}

// Pick the preferred route over a candidate span, returning its index (route_select_npos when the
// span is empty). Ranking: direct before relayed, then fewer hops before more, with first-in-span
// order as the deterministic tie-break — so the winner is independent of input order for a fixed set.
// Pure: no state, no allocation, no I/O.
inline std::size_t route_select(std::span<const route_candidate> candidates) noexcept
{
    std::size_t best = route_select_npos;
    for(std::size_t i = 0; i < candidates.size(); ++i)
        if(best == route_select_npos || route_outranks(candidates[i], candidates[best]))
            best = i;
    return best;
}

}

#endif
