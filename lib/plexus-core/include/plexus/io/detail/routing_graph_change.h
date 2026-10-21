#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_GRAPH_CHANGE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_GRAPH_CHANGE_H

#include "plexus/io/observer.h"

#include "plexus/io/detail/routing_reports.h"
#include "plexus/io/detail/routing_relay_emit.h"

#include "plexus/graph/graph_change.h"

#include "plexus/node_id.h"

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace plexus::io::detail {

// The posted wakeup: fires the coarse edge once at the FINAL generation, then drains the host
// delta log to the opt-in observers. It walks the live observer list by index, not through
// detail::fan_out: fan_out's snapshot copy would allocate on every wakeup and the coarse path must
// stay allocation-free (the [this] closure fits move_only_function's SBO), while indexing survives
// a callback that (un)registers an observer. It re-reads known_peers/topic_table only, never
// slot_block, so a peer torn down before the drain cannot be re-entered.
template<typename Engine>
void fire_graph_wakeup(Engine &e)
{
    e.m_graph_wakeup_pending = false;
    const std::uint64_t gen  = e.m_graph_generation;
    for(std::size_t i = 0; i < e.m_observers.size(); ++i)
        e.m_observers[i].get().on_graph_changed(gen);
    const auto deltas = e.m_graph_log.drain();
    if(deltas.empty())
        return;
    for(std::size_t i = 0; i < e.m_observers.size(); ++i)
    {
        observer &o = e.m_observers[i].get();
        if(o.observes_graph())
            for(const graph::graph_change &delta : deltas)
                o.on_graph_delta(delta);
    }
    e.m_graph_log.clear();
}

// Awareness-only removal on the existing tick: forgets a peer that stopped re-announcing and was
// not refreshed by a heartbeat. A reported origin ages out the same way — its topics retire with
// it (a live reporter is the only refresh). NEVER touches m_registry — an active session outlives
// its awareness entry.
template<typename Engine>
void sweep_aged_awareness(Engine &e)
{
    const std::uint64_t now      = e.now_for_aging();
    const std::uint64_t ttl      = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(e.m_liveliness.awareness_ttl).count());
    const std::uint64_t deadline = now > ttl ? now - ttl : 0;
    e.m_known_peers.expire_older_than(deadline,
                                      [&e](const node_id &id)
                                      {
                                          detail::release_all_reporter_load(e, id);
                                          e.m_peer_liveliness.note_awareness_lost(id);
                                          detail::relay_withdraw(e, id);
                                          if(e.m_reported.erase(id) != 0)
                                              e.m_topics.remove_node(id);
                                          e.bump_graph_generation(true, id, graph::change_kind::disappeared);
                                      });
}

}

#endif
