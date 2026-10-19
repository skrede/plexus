#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_ALWAYS_LIVE_SLOTS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_ALWAYS_LIVE_SLOTS_H

#include "plexus/shm/slot_liveness.h"

#include <cstdint>

namespace plexus::testing {

// The slot liveness of an in-memory stub region, whose holders never die: every hold succeeds and every slot reads
// held, so a producer over a stub region never reclaims a cursor.
struct always_live_slots
{
    plexus::shm::slot_hold hold_slot(std::uint32_t) noexcept
    {
        return plexus::shm::slot_hold::held;
    }

    void release_slot(std::uint32_t) noexcept
    {
    }

    bool slot_held_elsewhere(std::uint32_t) const noexcept
    {
        return true;
    }
};

}

#endif
