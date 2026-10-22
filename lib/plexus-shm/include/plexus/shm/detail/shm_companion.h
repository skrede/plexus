#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_COMPANION_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_COMPANION_H

#include "plexus/shm/detail/shm_mux_acquire.h"
#include "plexus/shm/detail/shm_fallback_reason.h"

#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/upgrade_channel.h"

#include "plexus/node_id.h"

#include <memory>
#include <string>
#include <cstdint>
#include <utility>

namespace plexus::shm::detail {

// A restarted publisher adopts the ring its co-host readers are still attached to.
template<typename Member>
std::unique_ptr<typename Member::channel_type> companion_send(Member &m, const std::string &fqn, shm::waiter_loss::lost_fn lost)
{
    return mux_open(m, io::endpoint{"shm", fqn}, shm::ring_role::producer, std::move(lost));
}

// The reader in the claimed slot gates the node's send ring for fqn from its newest message on; false when that ring is
// gone or the slot does not hold that claim's reader waiting for its placement.
template<typename Member>
bool companion_place(Member &m, const std::string &fqn, io::reader_claim reader)
{
    auto *channel = m.registry().channel_for(fqn, shm::ring_direction::request, m.self_id());
    return channel != nullptr && channel->place_reader(reader.slot, reader.generation);
}

// The handle is minted held: it takes nothing until it is resumed.
template<typename Member>
std::unique_ptr<typename Member::consumer_type> companion_receive(Member &m, const std::string &fqn, const node_id &publisher, shm::waiter_loss::lost_fn lost)
{
    auto &registry = m.registry();
    if(registry.acquire(fqn, shm::ring_direction::request, publisher, shm::ring_role::consumer, 0) == shm::acquire_result::failed)
        return nullptr;
    return std::make_unique<typename Member::consumer_type>(registry, fqn, publisher, std::move(lost));
}

}

#endif
