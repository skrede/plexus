#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_REGISTERED_WAITERS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_REGISTERED_WAITERS_H

#include "plexus/shm/ring_layout.h"

#include <atomic>
#include <cstdint>

namespace plexus::testing {

inline void register_waiters(plexus::shm::control_header_t &header, std::uint32_t count) noexcept
{
    header.consumer_capacity = count;
    header.high_water.store(count, std::memory_order_release);
    for(std::uint32_t slot = 0; slot < count; ++slot)
        header.cursors[slot].active.store(1, std::memory_order_release);
}

}

#endif
