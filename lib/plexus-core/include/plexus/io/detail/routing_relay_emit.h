#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_RELAY_EMIT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_RELAY_EMIT_H

#include "plexus/io/detail/routing_session_scan.h"

#include "plexus/wire/peer_report.h"

#include "plexus/node_id.h"

namespace plexus::io::detail {

template<typename Engine>
void relay_send(Engine &e, typename Engine::session_type &s, const wire::peer_report &pr)
{
    if(s.peer_identity() != pr.origin && !e.m_emitter.declines(s.peer_identity()))
        e.m_messages.send_peer_report(s.msg_peer().channel, wire::encode_peer_report(pr));
}

// Send one report to every connected session but the origin's own — an origin is never announced
// back to itself. The encode is per-broadcast, off the emitted report only. A closing relay sends
// none: its sessions end together, and a withdrawal would tell a peer the origin left when only
// this path did.
template<typename Engine>
void relay_broadcast(Engine &e, const wire::peer_report &pr)
{
    if(e.closing())
        return;
    auto bytes = wire::encode_peer_report(pr);
    e.m_registry.for_each_connected(
            [&](const node_id &, typename Engine::session_type &s)
            {
                if(s.peer_identity() != pr.origin && !e.m_emitter.declines(s.peer_identity()))
                    e.m_messages.send_peer_report(s.msg_peer().channel, bytes);
            });
}

template<typename Engine>
void relay_note_origin(Engine &e, const node_id &origin)
{
    e.m_emitter.note_origin(e.m_report, origin, e.m_report.universe, e.m_topics, [&e](const wire::peer_report &pr) { relay_broadcast(e, pr); });
}

template<typename Engine>
void relay_withdraw(Engine &e, const node_id &origin)
{
    e.m_emitter.withdraw(origin, [&e](const wire::peer_report &pr) { relay_broadcast(e, pr); });
}

// Re-assert every held report on the relay's heartbeat cadence so a downstream row for a
// still-live-but-idle origin refreshes before it ages out at awareness_ttl. Relay-only: the null
// emitter twin makes this a no-op, so a non-relay node still emits nothing.
template<typename Engine>
void relay_reassert(Engine &e)
{
    e.m_emitter.reassert([&e](const wire::peer_report &pr) { relay_broadcast(e, pr); });
}

// The emit seam a newly-completed session runs: hand it every origin already reported (skipping a
// report about its own peer) the way redeclare_all replays declarations, then lift the just-
// attached peer as an origin and assert it to the rest. A non-relay node threads the null twin, so
// both calls are no-ops and no peer_report leaves the wire.
template<typename Engine>
void relay_on_ready(Engine &e, const node_id &id)
{
    typename Engine::session_type *fresh = e.m_registry.session_for(id);
    if(fresh == nullptr)
        return;
    const node_id peer = fresh->peer_identity();
    e.m_emitter.replay(peer, [&e, fresh](const wire::peer_report &pr) { relay_send(e, *fresh, pr); });
    relay_note_origin(e, peer);
}

// Re-lift an already-attached origin whose topic table changed, so a late or updated declaration
// re-announces downstream. Only a directly-attached origin (never self, never a via-only reported
// candidate) is a relay source.
template<typename Engine>
void relay_maybe_refresh(Engine &e, const node_id &node)
{
    if(node == e.m_build.fsm_cfg.self_id || !detail::any_session_for(e, node))
        return;
    relay_note_origin(e, node);
}

// A peer's heartbeat carries its cooperative decline posture. Only a transition acts (the carrier is
// level): a fresh decline withdraws an already-emitted report and, held in the emitter, keeps a later
// note_origin from re-announcing it; an un-decline re-lifts the origin so its offer returns for free.
template<typename Engine>
void note_decline(Engine &e, const node_id &peer, bool declines)
{
    if(!e.m_emitter.mark_decline(peer, declines))
        return;
    if(declines)
        relay_withdraw(e, peer);
    else
        relay_maybe_refresh(e, peer);
}

}

#endif
