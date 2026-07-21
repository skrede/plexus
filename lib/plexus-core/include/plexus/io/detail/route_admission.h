#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTE_ADMISSION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTE_ADMISSION_H

#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include "plexus/detail/fail_closed.h"

#include <cstddef>
#include <algorithm>

namespace plexus::io::detail
{

struct admit_result
{
    bool changed;
    bool dropped;
};

// One direct row per identity: a re-store of the direct endpoint matches it regardless of transport,
// so a fresh direct address overwrites in place. A distinct transitive row is keyed by (via, transport).
inline bool same_candidate(const route_candidate &a, const route_candidate &b) noexcept
{
    if(a.is_direct() != b.is_direct())
        return false;
    if(a.is_direct())
        return true;
    return a.reach.via == b.reach.via && a.reach.transport == b.reach.transport;
}

inline void place(route_candidate &row, const route_candidate &cand, std::uint64_t now) noexcept
{
    row               = cand;
    row.last_refreshed = now;
}

// A direct candidate is never reject-and-counted: it refreshes its row, takes a free row, or reclaims
// a transitive row. Only an identity whose rows are ALL direct and full fails closed — the defined
// refusal for a target sized below its direct peer count.
inline admit_result admit_direct(route_candidate *slots, std::size_t &count, std::size_t cap, const route_candidate &cand, std::uint64_t now)
{
    for(std::size_t i = 0; i < count; ++i)
        if(slots[i].is_direct())
        {
            const bool changed = slots[i].reach.transport != cand.reach.transport;
            place(slots[i], cand, now);
            return {changed, false};
        }
    if(count < cap)
    {
        place(slots[count++], cand, now);
        return {true, false};
    }
    for(std::size_t i = 0; i < count; ++i)
        if(!slots[i].is_direct())
        {
            place(slots[i], cand, now);
            return {true, false};
        }
    plexus::detail::fail_closed("fixed_peer_storage: direct candidates exceed per-identity capacity");
}

inline bool transitive_has_room(const route_candidate *slots, std::size_t count, std::size_t cap, const route_options &opts) noexcept
{
    if(count >= cap)
        return false;
    if(opts.protection == direct_protection::priority_evict)
        return true;
    const std::size_t reserved = std::min(opts.reserved_direct, cap);
    std::size_t transitive     = 0;
    for(std::size_t i = 0; i < count; ++i)
        transitive += static_cast<std::size_t>(!slots[i].is_direct());
    return transitive < cap - reserved;
}

// A transitive candidate refreshes an existing row or takes a free one; when full it is rejected and
// counted, never granted room by evicting a live row (a direct row least of all).
inline admit_result admit_transitive(route_candidate *slots, std::size_t &count, std::size_t cap, const route_candidate &cand, std::uint64_t now, const route_options &opts)
{
    for(std::size_t i = 0; i < count; ++i)
        if(same_candidate(slots[i], cand))
        {
            slots[i].last_refreshed = now;
            return {false, false};
        }
    if(!transitive_has_room(slots, count, cap, opts))
        return {false, true};
    place(slots[count++], cand, now);
    return {true, false};
}

inline admit_result admit_candidate(route_candidate *slots, std::size_t &count, std::size_t cap, const route_candidate &cand, std::uint64_t now, const route_options &opts)
{
    if(cand.is_direct())
        return admit_direct(slots, count, cap, cand, now);
    return admit_transitive(slots, count, cap, cand, now, opts);
}

// The direct endpoint an identity's rows surface to a dial/scoping path: the direct row only, never a
// via-only candidate.
inline std::optional<endpoint> direct_endpoint(const route_candidate *slots, std::size_t count)
{
    for(std::size_t i = 0; i < count; ++i)
        if(slots[i].is_direct())
            return slots[i].reach.transport;
    return std::nullopt;
}

}

#endif
