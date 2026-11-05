#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SUBSCRIBE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SUBSCRIBE_H

#include "plexus/io/companion_event.h"

#include "plexus/io/detail/peer_session_gate.h"
#include "plexus/io/detail/peer_session_retry.h"

#include "plexus/wire/subscribe.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

inline bool is_refusal(wire::subscribe_status status)
{
    return status == wire::subscribe_status::type_mismatch || status == wire::subscribe_status::incompatible_qos || status == wire::subscribe_status::source_identity_incompatible ||
            status == wire::subscribe_status::unacknowledged;
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

// A refusal attached nothing, so it carries no offer to answer.
template<typename Session, typename Forwarder>
void note_offer(const Session &s, Forwarder &messages, const wire::subscribe_response &resp)
{
    if(!is_refusal(resp.status))
        messages.note_companion(s.msg_peer(), resp.topic_hash, offer_event_of(resp.offer));
}

// The anti-wrap guard stays first: a response with no outstanding match is a stray/duplicate/forged frame, dropped
// before any callback. Both injected hooks (the outcome callback, the companion hook in note_offer) may tear the
// session down, which zeroes m_outstanding_subscribes, so the decrement waits for both. A retrying session matches by
// topic, and a re-send makes a duplicate expected, so it is quiet.
template<typename Session>
void on_subscribe_response_received(Session &s, std::span<const std::byte> inner)
{
    auto resp = wire::decode_subscribe_response(inner);
    if(!resp)
        return report_malformed(s);
    if(retries(s.m_retry) && !take_unacked(s.m_retry, resp->topic_hash))
        return;
    if(s.m_outstanding_subscribes == 0)
        return s.m_logger.warn("plexus: subscribe_response with no outstanding match");
    surface_subscribe_outcome(s, *resp);
    if(!s.is_complete())
        return;
    note_offer(s, s.m_messages, *resp);
    if(!s.is_complete())
        return;
    --s.m_outstanding_subscribes;
    s.maybe_fire_ready();
}

}

#endif
