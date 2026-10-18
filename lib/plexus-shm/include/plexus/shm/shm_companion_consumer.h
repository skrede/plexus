#ifndef HPP_GUARD_PLEXUS_SHM_SHM_COMPANION_CONSUMER_H
#define HPP_GUARD_PLEXUS_SHM_SHM_COMPANION_CONSUMER_H

#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/node_id.h"

#include <string>
#include <utility>

namespace plexus::shm {

// The RAII receive lane over a co-host companion ring, drained by the registry's armed
// notifier (posted on the node executor, NO drain thread). Destruction clears the sink BEFORE
// dropping the refcount (disarm-before-destroy), so a wake in flight never delivers onto a
// torn-down sink and a release at 1->0 unmaps the ring. lost runs if the ring's waiter is lost.
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

    ~shm_companion_consumer()
    {
        m_registry.clear_consumer_sink(m_fqn, ring_direction::request, m_publisher);
        m_registry.release(m_fqn, ring_direction::request, m_publisher);
    }

private:
    registry_type &m_registry;
    node_id m_publisher;
    std::string m_fqn;
    loss_watch m_loss;
};

}

#endif
