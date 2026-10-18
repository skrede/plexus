#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_COMPANION_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_COMPANION_H

#include "plexus/shm/detail/shm_mux_acquire.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/upgrade_channel.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <string>
#include <cstddef>
#include <utility>

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

// The transport-neutral reason a failed acquire hands the coordinator, which never sees an acquire_bound.
inline io::fallback_reason fallback_of(const shm::acquire_failure &failure) noexcept
{
    switch(failure.bound)
    {
        case shm::acquire_bound::transient_join:
            return io::fallback_reason::transient;
        case shm::acquire_bound::incompatible:
            return io::fallback_reason::incompatible_layout;
        case shm::acquire_bound::slab_ceiling:
            return io::fallback_reason::slab_ceiling;
        case shm::acquire_bound::os_allocator:
            return io::fallback_reason::os_allocator;
        case shm::acquire_bound::notifier:
            return notifier_fallback(failure.arm);
        case shm::acquire_bound::none:
            break;
    }
    return io::fallback_reason::none;
}

// join_live: the co-host subscriber may have minted this companion ring first, so a clobbering unlink would split the
// pair onto two rings.
template<typename Member>
std::unique_ptr<typename Member::channel_type> companion_send(Member &m, const std::string &fqn, shm::waiter_loss::lost_fn lost)
{
    return mux_open(m, io::endpoint{"shm", fqn}, shm::acquire_mode::join_live, std::move(lost));
}

template<typename Member>
std::unique_ptr<typename Member::consumer_type> companion_receive(Member &m, const std::string &fqn, const node_id &publisher,
                                                                  plexus::detail::move_only_function<void(std::span<const std::byte>)> on_frame, shm::waiter_loss::lost_fn lost)
{
    auto &registry                = m.registry();
    const shm_resolved_geometry g = mux_resolve(m, fqn, shm::ring_direction::request);
    if(registry.acquire(fqn, shm::ring_direction::request, publisher, g.max_payload, g.mode, g.consumer_capacity, shm::acquire_mode::join_live) == shm::acquire_result::failed)
        return nullptr;
    auto handle = std::make_unique<typename Member::consumer_type>(registry, fqn, publisher, std::move(lost));
    registry.set_consumer_sink(fqn, shm::ring_direction::request, publisher,
                               [cb = std::move(on_frame)](::plexus::wire_bytes<shm::shm_slot_owner> wb) mutable { cb(std::span<const std::byte>{wb.data(), wb.size()}); });
    return handle;
}

}

#endif
