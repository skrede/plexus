#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTE_ADMISSION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTE_ADMISSION_H

#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include "plexus/wire/udp_dedup_window.h"

#include "plexus/node_id.h"

#include "plexus/detail/fail_closed.h"

#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::io::detail
{

struct admit_result
{
    bool changed;
    bool dropped;
};

enum class report_admit : std::uint8_t
{
    noted_new,
    refreshed,
    duplicate,
    dropped
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

// A reported (via-only) candidate keyed per (origin, via) row: an existing row admits the seq against
// its embedded dedup window, so a duplicate or too_old seq is a no-op that neither refreshes the row
// nor bumps the graph, and only a fresh seq refreshes it; a first sighting seeds the window and takes
// a non-reserved row (rejected-and-counted when the transitive region is full). Removal is
// remove_transitive_row, never this path.
inline report_admit admit_reported(route_candidate *slots, std::size_t &count, std::size_t cap, const route_candidate &cand, std::uint16_t seq, std::uint64_t now,
                                   const route_options &opts)
{
    for(std::size_t i = 0; i < count; ++i)
        if(same_candidate(slots[i], cand))
        {
            if(slots[i].seq_window.admit(seq) != wire::udp_dedup_window::outcome::fresh)
                return report_admit::duplicate;
            slots[i].last_refreshed = now;
            return report_admit::refreshed;
        }
    if(!transitive_has_room(slots, count, cap, opts))
        return report_admit::dropped;
    route_candidate seeded = cand;
    seeded.seq_window.anchor(seq);
    place(slots[count++], seeded, now);
    return report_admit::noted_new;
}

// Re-arm the dedup window of every transitive row reaching an origin via `via` back to its
// first-sighting state, so the reporter's next report re-anchors on its (possibly restarted) seq
// counter rather than measuring it against a stale high-water. Returns the number of rows re-armed.
inline std::size_t reset_windows_via(route_candidate *slots, std::size_t count, const node_id &via)
{
    std::size_t reset = 0;
    for(std::size_t i = 0; i < count; ++i)
        if(!slots[i].is_direct() && slots[i].reach.via == via)
        {
            slots[i].seq_window.reset();
            ++reset;
        }
    return reset;
}

// Admits a reported candidate into a record and stamps its freshness tick on a noted/refreshed row,
// so both storage twins share the whole per-record note path (only the drop tally stays per-storage).
inline report_admit note_reported_row(route_candidate *slots, std::size_t &count, std::size_t cap, std::uint64_t &last_refreshed, const route_candidate &cand,
                                      std::uint16_t seq, std::uint64_t now, const route_options &opts)
{
    const auto r = admit_reported(slots, count, cap, cand, seq, now, opts);
    if(r == report_admit::noted_new || r == report_admit::refreshed)
        last_refreshed = now;
    return r;
}

// Admit a seq against the dedup window of the transitive row reaching origin via `via` WITHOUT
// removing it: true only when fresh (a missing row is not fresh). Seq-validates a withdrawal so a
// replayed or reordered stale withdrawal cannot retire a live row.
inline bool admit_via_seq(route_candidate *slots, std::size_t count, const node_id &via, std::uint16_t seq)
{
    for(std::size_t i = 0; i < count; ++i)
        if(!slots[i].is_direct() && slots[i].reach.via == via)
            return slots[i].seq_window.admit(seq) == wire::udp_dedup_window::outcome::fresh;
    return false;
}

// Retires the transitive row reaching origin via `via`, compacting the array; the direct row (if any)
// is never touched. Returns true when a row was removed.
inline bool remove_transitive_row(route_candidate *slots, std::size_t &count, const node_id &via)
{
    for(std::size_t i = 0; i < count; ++i)
        if(!slots[i].is_direct() && slots[i].reach.via == via)
        {
            slots[i]       = slots[count - 1];
            slots[--count] = route_candidate{};
            return true;
        }
    return false;
}

// Flip the reachability of every transitive row reaching origin via `via` to `status` in place, WITHOUT
// removing it — the identity and its via edge survive so a returning relay recovers the row rather than
// re-minting it. Only a row that actually transitions is counted, so an idempotent re-mark reports no
// change. The direct row (always reachable) is never touched. Returns the number of rows re-marked.
inline std::size_t mark_reachability_via(route_candidate *slots, std::size_t count, const node_id &via, graph::reachability status)
{
    std::size_t changed = 0;
    for(std::size_t i = 0; i < count; ++i)
        if(!slots[i].is_direct() && slots[i].reach.via == via && slots[i].origin.reach_status != status)
        {
            slots[i].origin.reach_status = status;
            ++changed;
        }
    return changed;
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
