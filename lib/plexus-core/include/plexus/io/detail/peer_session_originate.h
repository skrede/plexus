#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_ORIGINATE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_ORIGINATE_H

#include "plexus/node_id.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <cstddef>

namespace plexus::io::detail {

// Re-originate a directly-attached publisher's PLAIN unidirectional publish as a forwarded envelope so it
// transits relay -> remote subscriber (the pub/sub analogue of the call -> forward_call fallback). It
// fires ONLY on the plain-receive path, never the forwarded (0x0F) path, so an already-forwarded frame is
// never re-originated and a just-re-fanned frame cannot re-enter as a plain publish. The origin is the
// HANDSHAKE-PROVEN peer identity (peer_identity() == last_seen_peer_id()), NEVER the provisional slot key
// m_ctx.peer_id — so an accepted publisher session stamps the publisher's real gid. wants_refan() is
// consulted BEFORE any header reconstruction, so a non-relay node does zero origination-path work.
template<typename Session>
void originate_if_pubsub(Session &s, const wire::frame_header &hdr, std::span<const std::byte> body)
{
    if(!s.m_messages.wants_refan() || !s.is_complete())
        return;
    const bool has_source_identity = (hdr.flags & wire::k_flag_source_identity) != 0;
    auto uni                       = wire::decode_unidirectional(body, has_source_identity);
    if(!uni)
        return;
    const std::vector<std::byte> header_on_inner = wire::encode_frame(hdr, body);
    s.m_messages.originate_forwarded(uni->header.topic_hash, s.peer_identity(), header_on_inner, &s.m_channel);
}

}

#endif
