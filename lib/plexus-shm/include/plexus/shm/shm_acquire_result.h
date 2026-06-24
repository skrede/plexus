#ifndef HPP_GUARD_PLEXUS_SHM_SHM_ACQUIRE_RESULT_H
#define HPP_GUARD_PLEXUS_SHM_SHM_ACQUIRE_RESULT_H

#include "plexus/shm/region_broker_concept.h"

#include <cstdint>

namespace plexus::shm {

enum class acquire_result : std::uint8_t
{
    created,
    attached,
    failed,
};

// How an acquire resolves a name collision with an existing region. reclaim_stale
// reclaims via unlink-then-create (the single-owner dial ring has one creator).
// join_live NEVER unlinks (clobbering a live co-host peer's region would split the
// pair onto two physical rings): it attaches first when the region exists, mints
// only when none does, so whichever peer arrives second JOINs.
enum class acquire_mode : std::uint8_t
{
    reclaim_stale,
    join_live,
};

enum class acquire_bound : std::uint8_t
{
    none,
    slab_ceiling,
    os_allocator,
};

struct acquire_failure
{
    acquire_bound bound       = acquire_bound::none;
    std::uint64_t ask_bytes   = 0;
    std::uint64_t limit_bytes = 0;                 // the slab ceiling (slab_ceiling bound only)
    region_status broker      = region_status::ok; // the OS verdict (os_allocator bound only)
};

}

#endif
