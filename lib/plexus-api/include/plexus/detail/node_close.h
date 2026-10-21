#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_CLOSE_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_CLOSE_H

#include "plexus/detail/node_self_carrier_sync.h"

#include "plexus/io/observation_events.h"
#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/close_notification.h"

#include <string>
#include <utility>

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
void notify_node_closed(Node &n)
{
    if(n.m_ops.settled())
        n.m_close_notification.notify([&n](auto notify) { Node::policy_type::post(n.m_executor, std::move(notify)); });
}

// The self channel counts as one operation of the node until its own notification.
template<typename Node>
void close_self_route(Node &n)
{
    if constexpr(Node::k_has_shm_self_carrier)
        while(!n.m_self_carriers.empty())
        {
            const std::string fqn = n.m_self_carriers.begin()->first;
            teardown_shm_self_carrier(n, fqn);
        }
    n.m_ops.begin();
    n.m_self_channel.on_closed([&ops = n.m_ops] { ops.end(); });
    n.m_self_channel.close();
}

// The sequence is itself one operation of the node, so the node settles only after it and after its engine and self
// channel have; a callback that closes again finds the node closing.
template<typename Node>
void close_node(Node &n)
{
    if(n.m_ops.closing())
        return;
    n.m_ops.begin();
    n.m_ops.close();
    n.m_ops.on_settled([&n] { notify_node_closed(n); });
    n.m_disc.stop();
    post_destroyed_edge(n);
    close_self_route(n);
    io::detail::hold_until_settled(n.m_ops, n.m_engine.pending());
    n.m_engine.close();
    n.m_ops.end();
}

}

#endif
