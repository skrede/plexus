#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_FALLBACK_REASON_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_FALLBACK_REASON_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/upgrade_channel.h"

namespace plexus::shm::detail {

inline io::fallback_reason notifier_fallback(shm::arm_status arm) noexcept
{
    switch(arm)
    {
        case shm::arm_status::denied:
            return io::fallback_reason::notifier_denied;
        case shm::arm_status::unsupported:
            return io::fallback_reason::notifier_unsupported;
        case shm::arm_status::exhausted:
            return io::fallback_reason::notifier_exhausted;
        case shm::arm_status::armed:
            break;
    }
    return io::fallback_reason::none;
}

// The lane stays engaged on the secondary wake; primary is why the preferred wake could not serve.
inline io::fallback_reason demoted_fallback(shm::arm_status primary) noexcept
{
    switch(primary)
    {
        case shm::arm_status::denied:
            return io::fallback_reason::notifier_demoted_denied;
        case shm::arm_status::unsupported:
            return io::fallback_reason::notifier_demoted_unsupported;
        case shm::arm_status::exhausted:
            return io::fallback_reason::notifier_demoted_exhausted;
        case shm::arm_status::armed:
            break;
    }
    return io::fallback_reason::none;
}

inline io::fallback_reason allocator_fallback(shm::region_status status) noexcept
{
    if(status == shm::region_status::no_space)
        return io::fallback_reason::no_space;
    return io::fallback_reason::os_allocator;
}

// The transport-neutral reason a failed acquire hands the coordinator, which never sees an acquire_bound.
inline io::fallback_reason fallback_of(const shm::acquire_failure &failure) noexcept
{
    switch(failure.bound)
    {
        case shm::acquire_bound::transient_join:
            return io::fallback_reason::transient;
        case shm::acquire_bound::incompatible:
            return io::fallback_reason::incompatible_layout;
        case shm::acquire_bound::consumer_capacity:
            return io::fallback_reason::consumer_capacity;
        case shm::acquire_bound::not_found:
            return io::fallback_reason::not_found;
        case shm::acquire_bound::liveness:
            return io::fallback_reason::liveness_unavailable;
        case shm::acquire_bound::slab_ceiling:
            return io::fallback_reason::slab_ceiling;
        case shm::acquire_bound::os_allocator:
            return allocator_fallback(failure.broker);
        case shm::acquire_bound::notifier:
            return notifier_fallback(failure.arm);
        case shm::acquire_bound::none:
            break;
    }
    return io::fallback_reason::none;
}

}

#endif
