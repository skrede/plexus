#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_DRAIN_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_DRAIN_H

#include "plexus/shm/detail/shm_topic_entry.h"
#include "plexus/shm/detail/shm_ring_lifetime.h"

#include <algorithm>

namespace plexus::shm::detail {

// The roles still held decide: none closes and erases the entry, no consumer disengages the cursor it still reads
// through, and no producer gives up the creator slot.
template<typename Broker, typename Entry>
void topic_settle(Broker &broker, shm_topic_map<Entry> &entries, typename shm_topic_map<Entry>::iterator it)
{
    Entry &e          = *it->second;
    e.release_pending = false;
    if(e.producers == 0 && e.consumers == 0)
    {
        ring_lifetime::close(broker, e);
        entries.erase(it);
        return;
    }
    if(e.consumers == 0 && e.reading())
        e.disengage_consumer();
    if(e.producers == 0)
        ring_lifetime::drop_creator(e);
}

// Every drain of an entry runs here. A release its delivery triggers only drops the role while the entry drains, and the
// outermost drain settles the entry once it returns, so nothing the drain runs on is destroyed under it.
template<typename Registry, typename Entry, typename Drain>
void topic_drain(Registry &r, Entry &e, Drain drain)
{
    ++e.drain_depth;
    drain();
    if(--e.drain_depth != 0 || !e.release_pending)
        return;
    auto it = std::find_if(r.m_entries.begin(), r.m_entries.end(), [&e](const auto &held) { return held.second.get() == &e; });
    if(it != r.m_entries.end())
        topic_settle(r.m_broker, r.m_entries, it);
}

}

#endif
