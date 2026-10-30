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

// Only a holder that consumes takes a reader cursor, which gates the producer, and arms a waiter; a consumer never
// creates the ring, so the publisher alone decides its geometry.
enum class ring_role : std::uint8_t
{
    producer,
    consumer,
    both,
};

// transient_join: a later join can succeed, because the ring's creator has not finished minting it (its slab is missing
// or its header unstamped), the ring is being retired, or every free reader slot is held for a moment; incompatible: the header carries a foreign layout; consumer_capacity: every reader
// slot of the ring is taken, so this acquirer could never receive; not_found: no ring exists under the name; liveness:
// the host gives this reader no kernel proof of its life, so a producer could not tell its cursor from a dead one;
// creator_alive: another holder has the ring's creator slot.
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
    liveness,
    creator_alive,
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
