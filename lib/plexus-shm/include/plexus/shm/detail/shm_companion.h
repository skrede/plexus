#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_COMPANION_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_COMPANION_H

#include "plexus/shm/detail/shm_mux_acquire.h"
#include "plexus/shm/detail/shm_fallback_reason.h"

#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/endpoint.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <string>
#include <cstddef>
#include <utility>

namespace plexus::shm::detail {

// join_live never unlinks a live ring under the co-host readers still attached to it.
template<typename Member>
std::unique_ptr<typename Member::channel_type> companion_send(Member &m, const std::string &fqn, shm::waiter_loss::lost_fn lost)
{
    return mux_open(m, io::endpoint{"shm", fqn}, shm::ring_role::producer, shm::acquire_mode::join_live, std::move(lost));
}

template<typename Member>
std::unique_ptr<typename Member::consumer_type> companion_receive(Member &m, const std::string &fqn, const node_id &publisher,
                                                                  plexus::detail::move_only_function<void(std::span<const std::byte>)> on_frame, shm::waiter_loss::lost_fn lost)
{
    auto &registry = m.registry();
    if(registry.acquire(fqn, shm::ring_direction::request, publisher, shm::ring_role::consumer, 0) == shm::acquire_result::failed)
        return nullptr;
    auto handle = std::make_unique<typename Member::consumer_type>(registry, fqn, publisher, std::move(lost));
    registry.set_consumer_sink(fqn, shm::ring_direction::request, publisher,
                               [cb = std::move(on_frame)](::plexus::wire_bytes<shm::shm_slot_owner> wb) mutable { cb(std::span<const std::byte>{wb.data(), wb.size()}); });
    return handle;
}

}

#endif
