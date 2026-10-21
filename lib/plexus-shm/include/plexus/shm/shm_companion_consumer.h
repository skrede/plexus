#ifndef HPP_GUARD_PLEXUS_SHM_SHM_COMPANION_CONSUMER_H
#define HPP_GUARD_PLEXUS_SHM_SHM_COMPANION_CONSUMER_H

#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/upgrade_channel.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <utility>

namespace plexus::shm {

// The RAII receive lane over a co-host companion ring, drained by the registry's armed
// notifier (posted on the node executor, NO drain thread). It is minted held, taking nothing
// until resume installs its sink. Destruction clears the sink BEFORE releasing its consumer hold
// (disarm-before-destroy), so a wake in flight never delivers onto a torn-down sink and the last
// release unmaps the ring. lost runs if the ring's waiter is lost.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_companion_consumer
{
public:
    using registry_type = shm_topic_registry<Broker, Notifier>;

    shm_companion_consumer(registry_type &registry, std::string fqn, const node_id &publisher, waiter_loss::lost_fn lost)
            : m_registry(registry)
            , m_publisher(publisher)
            , m_fqn(std::move(fqn))
            , m_loss(registry.watch_loss(m_fqn, ring_direction::request, m_publisher, std::move(lost)))
    {
    }

    shm_companion_consumer(const shm_companion_consumer &)            = delete;
    shm_companion_consumer &operator=(const shm_companion_consumer &) = delete;

    // Delivers what the ring holds now and every frame a later wake takes.
    void resume(plexus::detail::move_only_function<void(std::span<const std::byte>)> on_frame)
    {
        m_registry.set_consumer_sink(m_fqn, ring_direction::request, m_publisher,
                                     [cb = std::move(on_frame)](::plexus::wire_bytes<shm_slot_owner> wb) mutable { cb(std::span<const std::byte>{wb.data(), wb.size()}); });
    }

    // The ring slot this reader holds and its claim generation, which the publisher places at its switch.
    io::reader_claim reader_claim()
    {
        shm_channel<Notifier> *channel = m_registry.channel_for(m_fqn, ring_direction::request, m_publisher);
        return channel == nullptr ? io::k_no_reader : channel->waiter_claim().value_or(io::k_no_reader);
    }

    ~shm_companion_consumer()
    {
        m_registry.clear_consumer_sink(m_fqn, ring_direction::request, m_publisher);
        m_registry.release(m_fqn, ring_direction::request, m_publisher, ring_role::consumer);
    }

private:
    registry_type &m_registry;
    node_id m_publisher;
    std::string m_fqn;
    loss_watch m_loss;
};

}

#endif
