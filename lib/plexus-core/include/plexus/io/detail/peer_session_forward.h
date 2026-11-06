#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FORWARD_H

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/forward_gate.h"
#include "plexus/io/detail/peer_session_gate.h"
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
void refan_if_pubsub(Session &s, const wire::forwarded_frame &ff, const wire::frame_header &hdr)
{
    if(!s.m_messages.wants_refan() || hdr.type != wire::msg_type::unidirectional)
        return;
    const auto body = std::span<const std::byte>{ff.inner}.subspan(wire::header_size);
    auto uni        = wire::decode_unidirectional(body, (hdr.flags & wire::k_flag_source_identity) != 0);
    if(!uni)
        return report_malformed(s);
    const wire::shared_bytes *owner = nullptr;
    if constexpr(requires(typename Session::channel_type &c) { c.last_frame_owner(); })
        owner = s.m_channel.last_frame_owner();
    s.m_messages.refan_forwarded(uni->header.topic_hash, ff.origin, ff.hop, std::span<const std::byte>{ff.inner}, &s.m_channel, owner);
}

inline bool rpc_addressed_elsewhere(const wire::forwarded_frame &ff, const wire::frame_header &hdr, const node_id &self)
{
    const bool rpc = hdr.type == wire::msg_type::rpc_request || hdr.type == wire::msg_type::rpc_response;
    return rpc && ff.destination != node_id{} && ff.destination != self;
}

// Only a node with the engine's forward-rpc hook can re-resolve the frame onward; anywhere else it is unroutable.
template<typename Messages>
bool forward_rpc_onward(Messages &messages, const wire::forwarded_frame &ff)
{
    if(!messages.forward_rpc_installed())
        return false;
    messages.forward_rpc(ff.origin, ff.destination, ff.hop, std::span<const std::byte>{ff.inner});
    return true;
}

// Dispatch an admitted forwarded frame's header-on inner frame by its own type. A request/response
// addressed ELSEWHERE (destination set and not self) re-resolves onward by its destination identity
// through route_select at this hop, or is dropped as unroutable on a node that cannot forward — it is
// never served or completed locally. A frame addressed HERE (destination self or unset) delivers: a
// unidirectional lands with source = the trailer origin (never the relay session pin) only where this
// node demanded the topic from the relay session; a request stages the requester origin as its reply
// route; a response completes only a forwarded call, keyed by the responder ORIGIN rather than the
// arrival via, so a reply that re-resolved through a different relay still matches. Exactly one call per
// admitted frame.
template<typename Session>
bool dispatch_forwarded_inner(Session &s, const wire::forwarded_frame &ff, const wire::frame_header &hdr)
{
    const auto body = std::span<const std::byte>{ff.inner}.subspan(wire::header_size);
    if(rpc_addressed_elsewhere(ff, hdr, s.m_fsm_cfg.self_id))
    {
        if(!forward_rpc_onward(s.m_messages, ff))
            report_drop(s, drop_cause::unroutable, 0);
        return true;
    }
    switch(hdr.type)
    {
        case wire::msg_type::unidirectional:
            // A relayed publish addressed here is consumed only when the node uses relayed routes AND no
            // live direct session to this origin supersedes the relayed one; either posture drops it after
            // admission so a re-fanning relay still sees a sequenced frame and re-arm stays gap-free.
            if(!s.m_messages.consumes_relayed_from(ff.origin))
                return true;
            return deliver_data_with_source(s, hdr, body, ff.origin);
        case wire::msg_type::rpc_request:
            return s.m_procedures.deliver_request(s.m_rpc_peer, body, s.m_session_id, &ff.origin);
        case wire::msg_type::rpc_response:
            return s.m_procedures.deliver_forwarded_response(ff.origin, body);
        default:
            return true;
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
        return report_malformed(s);
    if(s.m_messages.admit_forwarded(*ff, s.m_fsm_cfg.self_id, arrival_relay) != forward_admission::admit)
        return;
    auto hdr = wire::decode_header(ff->inner);
    if(!hdr)
        return report_malformed(s);
    if(!dispatch_forwarded_inner(s, *ff, *hdr))
        return report_malformed(s);
    if(!s.is_complete())
        return;
    refan_if_pubsub(s, *ff, *hdr);
}

}

#endif
