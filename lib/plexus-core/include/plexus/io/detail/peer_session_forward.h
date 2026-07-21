#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H

#include "plexus/io/detail/forward_gate.h"
#include "plexus/io/detail/peer_session_deliver.h"

#include "plexus/node_id.h"

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

// Dispatch an admitted forwarded frame's header-on inner frame by its own type, applying the delivery-
// edge origin override on the data leg: a forwarded unidirectional delivers with source = the trailer
// origin (never the relay session pin), while rpc transit re-enters the procedure forwarder unchanged.
// Exactly one call per admitted frame (dedup upstream), so a forwarded frame delivers locally at most
// once. A non-data inner type is dropped — only the data-plane verbs transit.
template<typename Session>
void dispatch_forwarded_inner(Session &s, const node_id &origin, std::span<const std::byte> inner_frame)
{
    auto hdr = wire::decode_header(inner_frame);
    if(!hdr)
        return;
    const auto body = inner_frame.subspan(wire::header_size);
    switch(hdr->type)
    {
        case wire::msg_type::unidirectional:
            return deliver_data_with_source(s, *hdr, body, origin);
        case wire::msg_type::rpc_request:
            return s.m_procedures.deliver_request(s.m_rpc_peer, body, s.m_session_id);
        case wire::msg_type::rpc_response:
            return s.m_procedures.deliver_response(s.m_rpc_peer, body);
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
    dispatch_forwarded_inner(s, ff->origin, ff->inner);
    refan_if_pubsub(s, *ff);
}

}

#endif
