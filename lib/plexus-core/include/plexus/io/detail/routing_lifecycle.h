#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_LIFECYCLE_H

#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"

#include "plexus/io/detail/routing_reports.h"
#include "plexus/io/detail/routing_dispatch.h"

#include "plexus/graph/graph_change.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <string>

namespace plexus::io::detail {

template<typename Engine>
void register_endpoint(Engine &e, const node_id &id, const std::string &node_name)
{
    for(const auto &demand : e.m_messages.remembered_topics(node_name))
        e.m_monitor.register_endpoint(id, wire::fqn_topic_hash(demand.fqn), demand.qos.requested_deadline_ns, demand.qos.requested_lease_ns);
}

// A rejected handshake is an un-established session: tear_down's prior-complete guard suppresses
// the disconnected edge, so a dead/disconnected feed is NOT guaranteed to follow. Mapping rejected
// to session-down settles the peer as not-alive rather than leaving a stuck session state.
template<typename Engine>
void feed_liveliness_session(Engine &e, const lifecycle_event &ev)
{
    switch(ev.edge)
    {
        case lifecycle_edge::ready:
            register_endpoint(e, ev.id, ev.node_name);
            return e.m_peer_liveliness.note_session_up(ev.id);
        case lifecycle_edge::connected:
        case lifecycle_edge::reconnected:
            return e.m_peer_liveliness.note_session_up(ev.id);
        case lifecycle_edge::disconnected:
        case lifecycle_edge::dead:
            e.m_monitor.deregister_endpoint(ev.id);
            e.m_coordinator.on_peer_dead(ev.node_name);
            return e.m_peer_liveliness.note_session_down(ev.id, e.now_for_aging());
        case lifecycle_edge::rejected:
            return e.m_peer_liveliness.note_session_down(ev.id, e.now_for_aging());
    }
}

// A peer that both dials and accepts holds two sessions under two slot keys but one proven
// identity, and the table is keyed by that identity.
template<typename Engine>
bool any_session_for(Engine &e, const node_id &proven)
{
    bool found = false;
    e.m_registry.for_each_connected([&](const node_id &, typename Engine::session_type &s) { found = found || s.peer_identity() == proven; });
    return found;
}

// Topic knowledge is scoped to the sessions that carried it, so a peer's records leave with the
// last of them — mirroring how the registry drops its fan-out entries. Awareness is untouched:
// a peer that goes down stays discovered, it just stops asserting topics.
template<typename Engine>
void forget_topics_on_teardown(Engine &e, const lifecycle_event &ev)
{
    if(ev.edge != lifecycle_edge::disconnected && ev.edge != lifecycle_edge::dead)
        return;
    typename Engine::session_type *torn = e.m_registry.session_for(ev.id);
    if(torn != nullptr && !any_session_for(e, torn->peer_identity()))
    {
        const node_id who  = torn->peer_identity();
        e.relay_withdraw(who);
        e.m_emitter.clear_decline(who);
        mark_reports_via_unreachable(e, who);
        e.m_messages.resume_relayed_from(who);
        const bool changed = e.m_topics.remove_node(who);
        e.bump_graph_generation(changed, who, graph::change_kind::disappeared);
    }
}

inline bool is_session_live_edge(lifecycle_edge edge) noexcept
{
    return edge == lifecycle_edge::ready || edge == lifecycle_edge::connected || edge == lifecycle_edge::reconnected;
}

// A direct session to a currently-reported (relayed) origin just went live: retire its relayed
// delivery so the dual-homed consumer stops double-delivering. The lifecycle event carries the slot's
// provisional endpoint id, so the proven identity is read off the session the way relay_on_ready does.
template<typename Engine>
void arm_relayed_suppression(Engine &e, const node_id &slot_id)
{
    typename Engine::session_type *s = e.m_registry.session_for(slot_id);
    if(s == nullptr)
        return;
    const node_id proven = s->peer_identity();
    if(e.m_reported.count(proven) != 0)
        e.m_messages.suppress_relayed_from(proven);
}

template<typename Engine>
void dispatch_lifecycle(Engine &e, const lifecycle_event &ev)
{
    feed_liveliness_session(e, ev);
    forget_topics_on_teardown(e, ev);
    if(ev.edge == lifecycle_edge::connected || ev.edge == lifecycle_edge::reconnected)
    {
        reset_windows_for_reporter(e, ev.id);
        e.relay_on_ready(ev.id);
    }
    if(is_session_live_edge(ev.edge))
        arm_relayed_suppression(e, ev.id);
    post_lifecycle_edge(e, ev);
}

template<typename Engine>
void fan_liveness(Engine &e, const liveness_event &ev)
{
    if(e.m_on_liveness_cb)
        e.m_on_liveness_cb(ev);
}

}

#endif
