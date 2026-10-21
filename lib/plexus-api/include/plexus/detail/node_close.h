#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_CLOSE_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_CLOSE_H

#include "plexus/io/observation_events.h"

namespace plexus::detail {

// The peer watcher is deregistered first, so the destroyed edge's observer snapshot (captured in place of the engine,
// which may be destroyed before the executor runs the closure) holds only externally-owned observers.
template<typename Node>
void post_destroyed_edge(Node &n)
{
    n.m_engine.remove_observer(n.m_peer_watch);
    n.m_engine.post_participant_teardown({io::participant_edge::destroyed, n.m_id});
}

template<typename Node>
void close_node(Node &n)
{
    if(n.m_engine.closing())
        return;
    n.m_disc.stop();
    post_destroyed_edge(n);
    n.m_engine.close();
}

}

#endif
