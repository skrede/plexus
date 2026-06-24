#ifndef HPP_GUARD_PLEXUS_SHM_SHM_ACQUIRE_RESULT_H
#define HPP_GUARD_PLEXUS_SHM_SHM_ACQUIRE_RESULT_H

#include "plexus/shm/region_broker_concept.h"

#include <cstdint>

namespace plexus::shm {

// The outcome of an acquire: a topic ring is either freshly MINTED by this node (created — it
// owns the unlink), ATTACHED to a peer's existing ring (it never unlinks), or FAILED (the
// broker could not map a region; the caller falls back to the wire).
enum class acquire_result : std::uint8_t
{
    created,
    attached,
    failed,
};

// How an acquire resolves a name collision with an existing region. reclaim_stale is the
// per-peer dial/listen default: a create that races an existing name reclaims it via
// unlink-then-create (the single-owner dial ring has exactly one creator). join_live is the
// demand-driven COMPANION-convergence policy: two LIVE co-host peers independently acquire the
// SAME deterministically-named ring, so a create must NEVER unlink (clobbering the peer's live
// region would split the pair onto two physical rings). join_live attaches FIRST when the
// region exists, and mints only when none does yet; whichever peer arrives second JOINs.
enum class acquire_mode : std::uint8_t
{
    reclaim_stale,
    join_live,
};

// Which bound a fail-closed acquire hit. A reliable_preserving ring that cannot be provisioned
// NEVER silently downgrades to best_effort and NEVER silently sizes an unbounded region: the
// registration fails closed and names the bound here so the caller learns WHY.
//   none         -> no failure recorded (the success default).
//   slab_ceiling -> the ring's required bytes exceed the node-level slab ceiling.
//   os_allocator -> the broker / OS could not map the region.
enum class acquire_bound : std::uint8_t
{
    none,
    slab_ceiling,
    os_allocator,
};

// The unified fail-closed diagnostic a failed acquire records: which bound was hit, the exact
// ask (the ring's required slab bytes), and the available limit — the node-level slab ceiling
// for the ceiling bound, or the broker's region_status verdict for the OS-allocator bound.
struct acquire_failure
{
    acquire_bound bound       = acquire_bound::none;
    std::uint64_t ask_bytes   = 0;
    std::uint64_t limit_bytes = 0;                 // the slab ceiling (slab_ceiling bound only)
    region_status broker      = region_status::ok; // the OS verdict (os_allocator bound only)
};

}

#endif
