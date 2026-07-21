#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H

#include "plexus/io/detail/forward_gate.h"
#include "plexus/io/detail/peer_session_deliver.h"

#include "plexus/node_id.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/forwarded_frame.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

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
}

}

#endif
