#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_LIFECYCLE_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/disconnect_cause.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/routing_reports.h"
#include "plexus/io/detail/routing_dispatch.h"
#include "plexus/io/detail/peer_announcements.h"
#include "plexus/io/detail/routing_relay_emit.h"
#include "plexus/io/detail/routing_session_scan.h"

#include "plexus/graph/graph_change.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <string>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

template<typename Engine>
void register_endpoint(Engine &e, const node_id &id, const std::string &node_name)
{
    for(const auto &demand : e.m_messages.remembered_topics(node_name))
        e.m_monitor.register_endpoint(id, wire::fqn_topic_hash(demand.fqn), demand.qos.requested_deadline_ns, demand.qos.requested_lease_ns);
}

// A rejected handshake is an un-established session: tear_down's prior-complete guard suppresses
// the disconnected edge, so a dead/disconnected feed is NOT guaranteed to follow. Mapping rejected
// to session-down settles the peer as not-alive rather than leaving a stuck session state. An
// accepted session refused before it proved an identity names node_id{}, which no table tracks.
// A down edge leaves the tables alone while another complete session for the identity survives.
template<typename Engine>
void feed_liveliness_session(Engine &e, const lifecycle_event &ev)
{
    if(ev.id == node_id{})
        return;
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
            if(any_session_for(e, ev.id))
                return;
            e.m_monitor.deregister_endpoint(ev.id);
            e.m_coordinator.on_peer_dead(ev.node_name);
            return e.m_peer_liveliness.note_session_down(ev.id, e.now_for_aging());
        case lifecycle_edge::rejected:
            if(!any_session_for(e, ev.id))
                e.m_peer_liveliness.note_session_down(ev.id, e.now_for_aging());
            return;
    }
}

// Topic knowledge is scoped to the sessions that carried it, so a peer's records leave with the
// last of them — mirroring how the registry drops its fan-out entries. Awareness is untouched:
// a peer that goes down stays discovered, it just stops asserting topics. The dedup windows of
// frames it relayed leave too, because a relay that returns restarts its splice sequence at 0.
template<typename Engine>
void forget_topics_on_teardown(Engine &e, const lifecycle_event &ev)
{
    if(ev.edge != lifecycle_edge::disconnected && ev.edge != lifecycle_edge::dead)
        return;
    typename Engine::session_type *torn = e.m_registry.session_for(ev.id);
    const node_id who                   = torn != nullptr && torn->peer_identity() != node_id{} ? torn->peer_identity() : ev.id;
    if(!any_session_for(e, who))
    {
        detail::relay_withdraw(e, who);
        e.m_emitter.clear_decline(who);
        mark_reports_via_unreachable(e, who);
        e.m_messages.resume_relayed_from(who);
        e.m_messages.forward_dedup().reset_via(who);
        const bool changed = e.m_topics.remove_node(who);
        e.bump_graph_generation(changed, who, graph::change_kind::disappeared);
    }
}

inline bool is_session_live_edge(lifecycle_edge edge) noexcept
{
    return edge == lifecycle_edge::ready || edge == lifecycle_edge::connected || edge == lifecycle_edge::reconnected;
}

// A direct session to a currently-reported (relayed) origin just went live: retire its relayed
// delivery so the dual-homed consumer stops double-delivering. A dial by endpoint reports under the
// endpoint's id, so the proven identity is read off the session the way relay_on_ready does.
template<typename Engine>
void arm_relayed_suppression(Engine &e, const node_id &slot_id)
{
    typename Engine::session_type *s = detail::live_session_for(e, slot_id);
    if(s == nullptr)
        return;
    const node_id proven = s->peer_identity();
    if(e.m_reported.count(proven) != 0)
        e.m_messages.suppress_relayed_from(proven);
}

// Matched by label, not key: an accepted survivor waiting under a provisional key carries its label.
template<typename Engine>
bool carries_label(Engine &e, std::string_view node_name)
{
    bool found = false;
    e.registry().for_each_connected([&](const node_id &, typename Engine::session_type &s) { found = found || s.msg_peer().node_name == node_name; });
    return found;
}

template<typename Engine>
void release_identity(Engine &e, peer_announcements &announced, const node_id &id, std::string_view node_name)
{
    const std::optional<peer_kind> kind = announced.note_down(id);
    if(kind)
        post_disconnected(e, lifecycle_event{lifecycle_edge::disconnected, id, std::string{node_name}, *kind, handshake_outcome::none, disconnect_cause::unspecified});
}

// An observer sees an id go up when its first live session completes and down when its last one ends, so a handover
// between two sessions of one id posts nothing; the engine's own handlers still see every session's edge.
template<typename Engine>
void post_identity_edge(Engine &e, peer_announcements &announced, const lifecycle_event &ev)
{
    switch(ev.edge)
    {
        case lifecycle_edge::connected:
        case lifecycle_edge::reconnected:
            if(announced.note_up(ev.id, ev.kind))
                post_lifecycle_edge(e, ev);
            return;
        case lifecycle_edge::ready:
            if(announced.note_ready(ev.id))
                post_lifecycle_edge(e, ev);
            return;
        case lifecycle_edge::disconnected:
            if(!carries_label(e, ev.node_name) && announced.note_down(ev.id))
                post_lifecycle_edge(e, ev);
            return;
        case lifecycle_edge::rejected:
            if(ev.id != node_id{} && announced.is_up(ev.id) && !carries_label(e, ev.node_name))
                release_identity(e, announced, ev.id, ev.node_name);
            return post_lifecycle_edge(e, ev);
        case lifecycle_edge::dead:
            return post_lifecycle_edge(e, ev);
    }
}

// The loser closes without an edge, so an id its survivor does not carry is announced down here.
template<typename Engine>
void retire_superseded(Engine &e, const node_id &id, std::string_view node_name, std::string_view survivor_name)
{
    e.m_monitor.deregister_endpoint(id);
    e.m_coordinator.on_peer_dead(node_name);
    e.m_peer_liveliness.retire_session(id);
    if(node_name != survivor_name)
        release_identity(e, e.m_announced, id, node_name);
}

template<typename Engine>
void dispatch_lifecycle(Engine &e, const lifecycle_event &ev)
{
    if(is_misidentified_dial(ev))
        e.m_known_peers.invalidate_binding(ev.id);
    feed_liveliness_session(e, ev);
    forget_topics_on_teardown(e, ev);
    if(ev.edge == lifecycle_edge::connected || ev.edge == lifecycle_edge::reconnected)
    {
        reset_windows_for_reporter(e, ev.id);
        detail::relay_on_ready(e, ev.id);
    }
    if(is_session_live_edge(ev.edge))
        arm_relayed_suppression(e, ev.id);
    post_identity_edge(e, e.m_announced, ev);
}

template<typename Engine>
void fan_liveness(Engine &e, const liveness_event &ev)
{
    if(e.m_on_liveness_cb)
        e.m_on_liveness_cb(ev);
}

}

#endif
