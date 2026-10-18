#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SUBSCRIBE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SUBSCRIBE_H

#include "plexus/io/companion_event.h"

#include "plexus/wire/subscribe.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

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

// A refusal attached nothing, so it carries no offer to answer.
template<typename Session, typename Forwarder>
void note_offer(const Session &s, Forwarder &messages, const wire::subscribe_response &resp)
{
    if(s.is_complete() && !is_refusal(resp.status))
        messages.note_companion(s.msg_peer(), resp.topic_hash, resp.companion_offered ? companion_event::offered : companion_event::not_offered);
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
    note_offer(s, s.m_messages, *resp);
    --s.m_outstanding_subscribes;
    s.maybe_fire_ready();
}

}

#endif
