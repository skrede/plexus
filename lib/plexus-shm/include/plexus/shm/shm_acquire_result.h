#ifndef HPP_GUARD_PLEXUS_SHM_SHM_ACQUIRE_RESULT_H
#define HPP_GUARD_PLEXUS_SHM_SHM_ACQUIRE_RESULT_H

#include "plexus/shm/arm_status.h"
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

// Only a holder that consumes takes a reader cursor, which gates the producer, and arms a waiter; a consumer never
// creates the ring, so the publisher alone decides its geometry.
enum class ring_role : std::uint8_t
{
    producer,
    consumer,
    both,
};

// transient_join: a co-host creator has not finished minting the ring (its slab is missing or its header unstamped), so
// a later join can succeed; incompatible: the header carries a foreign or older layout; consumer_capacity: every reader
// slot of the ring is taken, so this acquirer could never receive; not_found: no ring exists under the name.
enum class acquire_bound : std::uint8_t
{
    none,
    slab_ceiling,
    os_allocator,
    notifier,
    transient_join,
    incompatible,
    consumer_capacity,
    not_found,
};

// limit_bytes is set for the slab_ceiling bound only, broker for os_allocator and a missing-slab transient_join only, arm
// for notifier only.
struct acquire_failure
{
    explicit acquire_failure(acquire_bound bound = acquire_bound::none, std::uint64_t ask_bytes = 0, std::uint64_t limit_bytes = 0, region_status broker = region_status::ok,
                             arm_status arm = arm_status::armed) noexcept
            : arm(arm)
            , bound(bound)
            , broker(broker)
            , ask_bytes(ask_bytes)
            , limit_bytes(limit_bytes)
    {
    }

    arm_status arm;
    acquire_bound bound;
    region_status broker;
    std::uint64_t ask_bytes;
    std::uint64_t limit_bytes;
};

}

#endif
