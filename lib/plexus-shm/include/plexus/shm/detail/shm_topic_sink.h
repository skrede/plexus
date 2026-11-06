#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_SINK_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_SINK_H

#include "plexus/shm/detail/shm_topic_drain.h"
#include "plexus/shm/detail/shm_topic_entry.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/waiter_loss.h"

#include <utility>

namespace plexus::shm::detail {

// The entry's wake-drain hands each pending message here. Installing it resumes the reader where the producer placed it,
// else where it attached, and delivers what the ring already holds from there.
template<typename Registry>
void topic_set_sink(Registry &r, const shm_topic_key &key, typename Registry::deliver_fn sink)
{
    auto it = r.m_entries.find(key);
    if(it == r.m_entries.end())
        return;
    auto &e = *it->second;
    e.sink  = std::move(sink);
    ++e.sink_generation;
    if(e.channel)
        e.channel->resume_subscriber();
    topic_drain(r, e, [&e] { e.deliver_pending(); });
}

// A drain running the cleared sink delivers nothing more, and the sink is destroyed once that drain returns.
template<typename Entry>
void topic_clear_sink(shm_topic_map<Entry> &entries, const shm_topic_key &key)
{
    auto it = entries.find(key);
    if(it == entries.end())
        return;
    it->second->sink = {};
    ++it->second->sink_generation;
}

template<typename Entry>
shm::shm_channel<typename Entry::notifier_type> *topic_channel(shm_topic_map<Entry> &entries, const shm_topic_key &key)
{
    auto it = entries.find(key);
    if(it == entries.end() || !it->second->channel || it->second->loss->status() != shm::arm_status::armed)
        return nullptr;
    return &*it->second->channel;
}

template<typename Entry>
shm::loss_watch topic_watch_loss(shm_topic_map<Entry> &entries, const shm_topic_key &key, shm::waiter_loss::lost_fn lost)
{
    auto it = entries.find(key);
    return shm::loss_watch{it == entries.end() ? nullptr : it->second->loss, std::move(lost)};
}

}

#endif
