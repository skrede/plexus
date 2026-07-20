#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_COMPLETE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_COMPLETE_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/host_fingerprint.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/peer_session_handshake.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_declaration.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

// Forward-declared: execute() calls it before its definition below.
template<typename Session>
void on_complete(Session &s);

template<typename Session>
peer_kind session_kind(const Session &s) noexcept
{
    return s.m_is_inbound_bootstrap ? peer_kind::accepted : peer_kind::dialed;
}

template<typename Session>
void fire_lifecycle(Session &s, lifecycle_edge edge, handshake_outcome reason = handshake_outcome::none)
{
    if(!s.m_on_lifecycle_cb)
        return;
    s.m_on_lifecycle_cb(lifecycle_event{edge, s.m_ctx.peer_id, s.m_ctx.node_name, session_kind(s), reason});
}

template<typename Session>
void fire_security(Session &s, security_kind kind, security_cause cause = security_cause::none)
{
    if(!s.m_on_security_cb)
        return;
    s.m_on_security_cb(security_event{kind, s.m_ctx.peer_id, cause});
}

// has_ever_connected survives teardown: read its prior value, then latch it true — the read
// MUST precede the set, so the first complete fires connected and later ones reconnected.
template<typename Session>
void fire_connect_edge(Session &s)
{
    const bool first           = !s.m_ctx.has_ever_connected;
    s.m_ctx.has_ever_connected = true;
    fire_lifecycle(s, first ? lifecycle_edge::connected : lifecycle_edge::reconnected);
}

// A transport that already provides authenticated encryption; the AEAD decorator decorates
// only plaintext network channels.
template<typename Session>
bool channel_is_self_securing(const Session &s)
{
    const auto scheme = s.m_channel.remote_endpoint().scheme;
    return scheme == "tls" || scheme == "dtls";
}

template<typename Session>
void refuse_posture(Session &s)
{
    s.m_negotiator.clear_identity();
    fire_lifecycle(s, lifecycle_edge::rejected, handshake_outcome::reject_unauthorized);
    fire_security(s, security_kind::posture_mismatch);
    s.close_for_protocol_error(wire::close_cause::invalid_magic);
}

// Fire rejected carrying the refusal reason BEFORE the close. An abort is an un-established
// session, so tear_down's prior-complete guard suppresses disconnected — only rejected fires.
template<typename Session>
void on_abort(Session &s, handshake_outcome reason)
{
    fire_lifecycle(s, lifecycle_edge::rejected, reason);
    if(reason == handshake_outcome::reject_unauthorized)
        fire_security(s, s.m_negotiator.classify_unauthorized());
    s.close_for_protocol_error(wire::close_cause::invalid_magic);
}

template<typename Session>
void execute(Session &s, const fsm_step_result &step)
{
    switch(step.action)
    {
        case fsm_action::send_request:
            return send_handshake_request(s);
        case fsm_action::send_response:
            return send_handshake_response(s, step.outcome);
        case fsm_action::complete:
            return on_complete(s);
        case fsm_action::retry:
            return s.m_logger.warn("plexus: peer_session retry_no_dialer");
        case fsm_action::abort:
            return on_abort(s, step.outcome);
        case fsm_action::none:
            return;
    }
}

// A null/absent peer fingerprint is conservatively NOT same-host.
template<typename Session>
void record_same_host(Session &s) noexcept
{
    s.m_ctx.same_host = is_same_host(s.m_fsm.last_seen_peer_fingerprint(), s.m_fsm.local_fingerprint());
}

// Re-emit each remembered subscribe through the counted subscribe(), so every 0->1 wire-emit
// increments the outstanding count and the demanded subscribes are outstanding when ready is
// evaluated (the first-publish-loss guard). Demand survives teardown; only unsubscribe forgets it.
template<typename Session>
void resubscribe_all(Session &s)
{
    for(const auto &demand : s.m_messages.remembered_topics(s.m_ctx.node_name))
        s.subscribe(demand.fqn, demand.qos, demand.type_id, demand.type_name);
}

// A declaration emitted before the session completes is lost, and a peer that reconnects has
// forgotten everything it was told: re-assert every local topic here, for the same reason the
// demand above replays. Declarations survive teardown; only a re-declare rewrites one.
template<typename Session>
void redeclare_all(Session &s)
{
    s.m_messages.for_each_local_declaration([&s](const wire::topic_declaration &td) { s.declare(td); });
}

// Install-once: a second complete (the simultaneous-connect tail) no-ops. A bootstrap inbound
// still sends the accept response — that is how the dialer learns it was accepted and mints its
// own epoch.
template<typename Session>
void on_complete(Session &s)
{
    if(s.m_forwarders_installed)
        return;
    if(s.m_is_inbound_bootstrap)
        send_handshake_response(s, handshake_outcome::accept_inbound);
    s.m_handshake_timer.cancel();
    s.m_session_id = s.m_ctx.epochs.next();
    record_same_host(s);
    if(!s.m_negotiator.install_on_accept(channel_is_self_securing(s)))
        return refuse_posture(s);
    s.m_forwarders_installed = true;
    fire_connect_edge(s);
    resubscribe_all(s);
    redeclare_all(s);
    s.maybe_fire_ready();
}

inline bool is_refusal(wire::subscribe_status status)
{
    return status == wire::subscribe_status::type_mismatch || status == wire::subscribe_status::incompatible_qos || status == wire::subscribe_status::source_identity_incompatible;
}

template<typename Session>
void surface_subscribe_outcome(Session &s, const wire::subscribe_response &resp)
{
    if(is_refusal(resp.status))
    {
        if(s.m_on_subscribe_refused_cb)
            s.m_on_subscribe_refused_cb(resp.topic_hash, resp.status);
    }
    else if(resp.status == wire::subscribe_status::subscribed_degraded)
    {
        if(s.m_on_subscribe_degraded_cb)
            s.m_on_subscribe_degraded_cb(resp.topic_hash, resp.degraded_flags);
    }
}

// The anti-wrap guard stays first: a response with no outstanding match is a
// stray/duplicate/forged frame, dropped before anything else so it can never fire a callback
// nor wrap m_outstanding_subscribes (which would make ready unreachable for the cycle).
template<typename Session>
void on_subscribe_response_received(Session &s, std::span<const std::byte> inner)
{
    auto resp = wire::decode_subscribe_response(inner);
    if(!resp)
        return;
    if(s.m_outstanding_subscribes == 0)
        return s.m_logger.warn("plexus: subscribe_response with no outstanding match");
    surface_subscribe_outcome(s, *resp);
    --s.m_outstanding_subscribes;
    s.maybe_fire_ready();
}

}

#endif
