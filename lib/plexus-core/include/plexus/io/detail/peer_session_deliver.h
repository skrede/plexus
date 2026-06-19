#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DELIVER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DELIVER_H

#include "plexus/io/message_info.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <cstddef>
#include <string_view>

namespace plexus::io::detail {

// Stamp the header-derived metadata at the ONE point the decoded frame_header is still
// live alongside the data (the router strips it before deliver). source_timestamp is the
// publisher's wire stamp; reception_timestamp is receiver-stamped from the same clock the
// codec uses; from_intra_process is the channel's latched tier verdict.
template<typename Session>
message_info assemble_message_info(Session &s, const wire::frame_header &hdr)
{
    message_info info{};
    info.source_timestamp    = hdr.timestamp_ns;
    info.reception_timestamp = wire::now_timestamp_ns();
    info.from_intra_process  = s.m_from_intra_process;
    return info;
}

// Precedence: a per-session info/bytes seam (the test/cold-path install) wins when set;
// otherwise the node-shared route (threaded by the registry, so it survives a reconnect
// rebuild) delivers. has_source_identity is read from the gid flag and passed to BOTH
// deliver paths: the producer emits the varint counter per ITS topic declaration, so even
// the bytes-only 2-arg path must honor the flag to land the data span correctly. The gid's
// node_id half is m_ctx.peer_id — the PINNED session peer — never a node_id from the frame.
// None set = silent drop. RELOCATION of the session body (a friend).
template<typename Session>
void deliver_session_data(Session &s, const wire::frame_header &hdr,
                          std::span<const std::byte> inner)
{
    const bool has_source_identity = (hdr.flags & wire::k_flag_source_identity) != 0;
    if(s.m_on_message_with_info)
    {
        const message_info info = assemble_message_info(s, hdr);
        s.m_messages.deliver(
                s.m_msg_peer, inner, info, s.m_ctx.peer_id, has_source_identity,
                [&s](std::string_view fqn, std::span<const std::byte> data, const message_info &mi)
                { s.m_on_message_with_info(fqn, data, mi); });
        return;
    }
    if(s.m_on_message)
    {
        s.m_messages.deliver(s.m_msg_peer, inner, s.m_ctx.peer_id, has_source_identity,
                             [&s](std::string_view fqn, std::span<const std::byte> data)
                             { s.m_on_message(fqn, data); });
        return;
    }
    if(s.m_on_message_route)
    {
        const message_info info = assemble_message_info(s, hdr);
        s.m_messages.deliver(s.m_msg_peer, inner, info, s.m_ctx.peer_id, has_source_identity,
                             [&s](std::string_view fqn, std::span<const std::byte> data,
                                  const message_info &mi) { s.m_on_message_route(fqn, data, mi); });
        return;
    }
    s.m_messages.deliver(s.m_msg_peer, inner, s.m_ctx.peer_id, has_source_identity,
                         [](std::string_view, std::span<const std::byte>) {});
}

// The object-lane receive tail: resolve the carrier's topic_hash to its fqn (recorded at
// demand time), hand it up the route, then RELEASE the reference the channel delivered. An
// unresolvable hash is warn-and-dropped and still released — never delivered, never leaked.
// No session_id staleness gate: this lane never crosses a reconnect boundary.
template<typename Session>
void deliver_session_object(Session &s, const object_carrier &c)
{
    auto fqn = s.m_messages.fqn_for(c.topic_hash);
    if(fqn.empty())
    {
        s.m_logger.warn("plexus: peer_session object_topic_unknown");
        return release(c);
    }
    if(s.m_on_object_route)
        s.m_on_object_route(fqn, c);
    release(c);
}

}

#endif
