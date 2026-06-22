#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_COMPLETE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_COMPLETE_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/security_event.h"
#include "plexus/io/detail/peer_session_handshake.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

// Forward-declared because execute() calls it before its definition below.
template<typename Session>
void on_complete(Session &s);

// The peer_kind discriminator: an inbound bootstrap is an accepted peer, an outbound
// dial a dialed peer.
template<typename Session>
peer_kind session_kind(const Session &s) noexcept
{
    return s.m_is_inbound_bootstrap ? peer_kind::accepted : peer_kind::dialed;
}

// Route one lifecycle edge up the seam (if wired) as an owned-value carrier.
template<typename Session>
void fire_lifecycle(Session &s, lifecycle_edge edge,
                    handshake_outcome reason = handshake_outcome::none)
{
    if(!s.m_on_lifecycle)
        return;
    s.m_on_lifecycle(
            lifecycle_event{edge, s.m_ctx.peer_id, s.m_ctx.node_name, session_kind(s), reason});
}

// Route one security event up the dedicated seam (if wired) carrying THIS peer's pinned id.
// The cause is meaningful only on the stream-tamper kind.
template<typename Session>
void fire_security(Session &s, security_kind kind, security_cause cause = security_cause::none)
{
    if(!s.m_on_security)
        return;
    s.m_on_security(security_event{kind, s.m_ctx.peer_id, cause});
}

// Fire connected on the first-ever complete, reconnected on every subsequent one. The
// discriminator is the long-lived has_ever_connected flag on the record (it survives
// teardown): read its PRIOR value, then latch it true. The read MUST precede the set.
template<typename Session>
void fire_connect_edge(Session &s)
{
    const bool first           = !s.m_ctx.has_ever_connected;
    s.m_ctx.has_ever_connected = true;
    fire_lifecycle(s, first ? lifecycle_edge::connected : lifecycle_edge::reconnected);
}

// A transport that already provides authenticated encryption: its scheme names a crypto
// handshake. The AEAD decorator decorates ONLY plaintext network channels.
template<typename Session>
bool channel_is_self_securing(const Session &s)
{
    const auto scheme = s.m_channel.remote_endpoint().scheme;
    return scheme == "tls" || scheme == "dtls";
}

// The fail-closed posture refusal: clear the latched identity, fire the posture security
// event + the lifecycle rejected edge, and tear down — no silent plaintext fallback.
template<typename Session>
void refuse_posture(Session &s)
{
    s.m_negotiator.clear_identity();
    fire_lifecycle(s, lifecycle_edge::rejected, handshake_outcome::reject_unauthorized);
    fire_security(s, security_kind::posture_mismatch);
    s.close_for_protocol_error(wire::close_cause::invalid_magic);
}

// An FSM abort: fire rejected carrying the real refusal reason BEFORE the close. The abort
// path is an un-established session, so tear_down's prior-complete guard suppresses
// disconnected — only rejected fires.
template<typename Session>
void on_abort(Session &s, handshake_outcome reason)
{
    fire_lifecycle(s, lifecycle_edge::rejected, reason);
    if(reason == handshake_outcome::reject_unauthorized)
        fire_security(s, s.m_negotiator.classify_unauthorized());
    s.close_for_protocol_error(wire::close_cause::invalid_magic);
}

// Map an FSM action to channel I/O. retry stays the FSM's intent as a log line: the
// reconnect driver lives outside the session and observes the dial-failure directly.
template<typename Session>
void execute(Session &s, const fsm_step_result &step)
{
    switch(step.action)
    {
        case fsm_action::send_request:  return send_handshake_request(s);
        case fsm_action::send_response: return send_handshake_response(s, step.outcome);
        case fsm_action::complete:      return on_complete(s);
        case fsm_action::retry:         return s.m_logger.warn("plexus: peer_session retry_no_dialer");
        case fsm_action::abort:         return on_abort(s, step.outcome);
        case fsm_action::none:          return;
    }
}

// The same-host eligibility verdict: compare the peer's advertised fingerprint to our own
// (a null/absent peer fingerprint is conservatively NOT same-host) and record it. This is
// the ELIGIBILITY gate the shared-memory upgrade reads.
template<typename Session>
void record_same_host(Session &s) noexcept
{
    s.m_ctx.same_host =
            shm::is_same_host(s.m_fsm.last_seen_peer_fingerprint(), s.m_fsm.local_fingerprint());
}

// Re-emit every remembered subscribe for this peer through the counted subscribe(), so each
// 0->1 wire-emit increments the outstanding count. The demand survives a teardown — only a
// genuine unsubscribe forgets it. Carries the remembered type_id so a typed subscriber
// surviving a reconnect re-subscribes with the SAME type gate.
template<typename Session>
void resubscribe_all(Session &s)
{
    for(const auto &demand : s.m_messages.remembered_topics(s.m_ctx.node_name))
        s.subscribe(demand.fqn, demand.qos, demand.type_id);
}

// Install-once: a second complete (the simultaneous-connect tail) no-ops. A bootstrap
// inbound completes with no counter-dial in flight, so it must still send the accept
// response: that is how the dialer learns it was accepted and mints its own epoch. The
// resubscribe_all runs on EVERY completion (first connect and reconnect) through the counted
// path, so the demanded subscribes are outstanding when ready is evaluated — the
// first-publish-loss guard. The simultaneous-connect path sends no redundant response.
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
    s.maybe_fire_ready();
}

static bool is_refusal(wire::subscribe_status status)
{
    return status == wire::subscribe_status::type_mismatch ||
            status == wire::subscribe_status::incompatible_qos ||
            status == wire::subscribe_status::source_identity_incompatible;
}

// Fire the subscribe-outcome observables for a matched response (kept separate so the
// handler reads as guard -> surface -> decrement).
template<typename Session>
void surface_subscribe_outcome(Session &s, const wire::subscribe_response &resp)
{
    if(is_refusal(resp.status))
    {
        if(s.m_on_subscribe_refused)
            s.m_on_subscribe_refused(resp.topic_hash, resp.status);
    }
    else if(resp.status == wire::subscribe_status::subscribed_degraded)
    {
        if(s.m_on_subscribe_degraded)
            s.m_on_subscribe_degraded(resp.topic_hash, resp.degraded_flags);
    }
}

// The consumer half of the subscribe loop: an arriving subscribe_response drives the
// readiness decrement. The anti-wrap guard stays FIRST: a response with no outstanding match
// is a stray/duplicate/forged frame, warned-and-dropped BEFORE anything else, so it can never
// fire a callback nor wrap the uint16_t (which would make ready unreachable for the cycle).
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
