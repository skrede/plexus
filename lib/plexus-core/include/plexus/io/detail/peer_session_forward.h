#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H

#include "plexus/io/node_name.h"

#include "plexus/io/detail/forward_gate.h"
#include "plexus/io/detail/peer_session_deliver.h"

#include "plexus/node_id.h"

#include <string>

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/forwarded_frame.h"
#include "plexus/wire/frame_reassembler.h"

#include <span>
#include <cstddef>
#include <type_traits>

namespace plexus::io::detail {

// Re-fan an admitted forwarded PUB/SUB frame onward to this relay's own subscribers. Only the data leg
// transits the splice (rpc reply routing is its own path); a relay that installed no splice seam is a
// pure local delivery and never decodes here. The outbound owner-carrying zero-copy is host-only: the
// receive seam is capability-probed for an inbound frame owner, and the default pooled copy is taken
// (owner nullptr) when the probe is absent, so the byte layout is identical without it.
template<typename Session>
void refan_if_pubsub(Session &s, const wire::forwarded_frame &ff)
{
    if(!s.m_messages.wants_refan())
        return;
    auto hdr = wire::decode_header(ff.inner);
    if(!hdr || hdr->type != wire::msg_type::unidirectional)
        return;
    const auto body = std::span<const std::byte>{ff.inner}.subspan(wire::header_size);
    auto uni        = wire::decode_unidirectional(body, (hdr->flags & wire::k_flag_source_identity) != 0);
    if(!uni)
        return;
    const wire::shared_bytes *owner = nullptr;
    if constexpr(requires(typename Session::channel_type &c) { c.last_frame_owner(); })
        owner = s.m_channel.last_frame_owner();
    s.m_messages.refan_forwarded(uni->header.topic_hash, ff.origin, ff.hop, std::span<const std::byte>{ff.inner}, &s.m_channel, owner);
}

// Dispatch an admitted forwarded frame's header-on inner frame by its own type. A request/response
// addressed ELSEWHERE (destination set and not self) re-resolves onward by its destination identity
// through route_select at this hop — the relay holds no correlation, so nothing is delivered locally.
// A frame addressed HERE (destination self or unset) delivers: a unidirectional lands with source =
// the trailer origin (never the relay session pin); a request stages the requester origin as its
// reply route; a response keys the pending table by the responder ORIGIN, not the arrival via, so a
// reply that re-resolved through a different relay still matches. Exactly one call per admitted frame.
template<typename Session>
void dispatch_forwarded_inner(Session &s, const wire::forwarded_frame &ff)
{
    auto hdr = wire::decode_header(ff.inner);
    if(!hdr)
        return;
    const auto body      = std::span<const std::byte>{ff.inner}.subspan(wire::header_size);
    const bool elsewhere = ff.destination != node_id{} && ff.destination != s.m_fsm_cfg.self_id;
    if(elsewhere && (hdr->type == wire::msg_type::rpc_request || hdr->type == wire::msg_type::rpc_response) && s.m_messages.forward_rpc_installed())
        return (void)s.m_messages.forward_rpc(ff.origin, ff.destination, ff.hop, std::span<const std::byte>{ff.inner});
    switch(hdr->type)
    {
        case wire::msg_type::unidirectional:
            // A relayed publish addressed here is consumed only when the node uses relayed routes; the
            // never posture drops it after admission so a re-fanning relay still sees a sequenced frame.
            if(!s.m_messages.consumes_relayed())
                return;
            return deliver_data_with_source(s, *hdr, body, ff.origin);
        case wire::msg_type::rpc_request:
            return s.m_procedures.deliver_request(s.m_rpc_peer, body, s.m_session_id, &ff.origin);
        case wire::msg_type::rpc_response:
        {
            const std::string key = node_name_of(ff.origin);
            return s.m_procedures.deliver_response(s.m_rpc_peer, body, &key);
        }
        default:
            return;
    }
}

// The forwarded receive seam: decode off the untrusted session frame, run the pure admission gate
// (self/loop, hop, (origin, arrival-relay) dedup) against the shared forwarder dedup state — attributing
// the SENDING relay as the handshake-proven identity for bookkeeping, never the provisional slot key —
// and deliver only on admit. A malformed or dropped frame delivers nothing.
template<typename Session>
void deliver_forwarded_frame(Session &s, const node_id &arrival_relay, std::span<const std::byte> inner)
{
    auto ff = wire::decode_forwarded_frame(inner);
    if(!ff)
        return;
    if(s.m_messages.admit_forwarded(*ff, s.m_fsm_cfg.self_id, arrival_relay) != forward_admission::admit)
        return;
    dispatch_forwarded_inner(s, *ff);
    refan_if_pubsub(s, *ff);
}

}

#endif
