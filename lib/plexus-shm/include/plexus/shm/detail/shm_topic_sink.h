#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_SINK_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_SINK_H

#include "plexus/shm/detail/shm_topic_entry.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"

#include <string>
#include <utility>

namespace plexus::shm::detail {

// The entry's wake-drain hands each pending message here instead of discarding; any
// already-queued message is delivered NOW so a late-attaching consumer misses nothing.
template<typename Entry>
void topic_set_sink(shm_topic_map<Entry> &entries, const std::string &fqn, shm::ring_direction direction, typename Entry::deliver_fn sink)
{
    auto it = entries.find(shm_topic_key{fqn, direction});
    if(it == entries.end())
        return;
    it->second->sink = std::move(sink);
    it->second->deliver_pending();
}

template<typename Entry>
void topic_clear_sink(shm_topic_map<Entry> &entries, const std::string &fqn, shm::ring_direction direction)
{
    auto it = entries.find(shm_topic_key{fqn, direction});
    if(it != entries.end())
        it->second->sink = {};
}

template<typename Entry>
shm::shm_channel<typename Entry::notifier_type> *topic_channel(shm_topic_map<Entry> &entries, const std::string &fqn, shm::ring_direction direction)
{
    auto it = entries.find(shm_topic_key{fqn, direction});
    if(it == entries.end() || !it->second->channel || it->second->loss->status() != shm::arm_status::armed)
        return nullptr;
    return &*it->second->channel;
}

template<typename Entry>
shm::loss_watch topic_watch_loss(shm_topic_map<Entry> &entries, const std::string &fqn, shm::ring_direction direction, shm::waiter_loss::lost_fn lost)
{
    auto it = entries.find(shm_topic_key{fqn, direction});
    return shm::loss_watch{it == entries.end() ? nullptr : it->second->loss, std::move(lost)};
}

}

#endif
