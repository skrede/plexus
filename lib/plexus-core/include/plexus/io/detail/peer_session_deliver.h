#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DELIVER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_DELIVER_H

#include "plexus/io/message_info.h"

#include "plexus/node_id.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <cstddef>
#include <string_view>

namespace plexus::io::detail {

// Stamped at the one point the decoded frame_header is still live alongside the data (the
// router strips it before deliver). reception_timestamp uses the same clock the codec uses.
template<typename Session>
message_info assemble_message_info(Session &s, const wire::frame_header &hdr)
{
    message_info info{};
    info.source_timestamp    = hdr.timestamp_ns;
    info.reception_timestamp = wire::now_timestamp_ns();
    info.from_intra_process  = s.m_from_intra_process;
    return info;
}

// A per-session seam wins when set; otherwise the node-shared route delivers. Every path must
// honor has_source_identity to land the data span correctly, and attributes the passed source
// (never a node_id read from the frame body). None set = silent drop.
template<typename Session>
// NOLINTNEXTLINE(readability-function-size)
void deliver_data_with_source(Session &s, const wire::frame_header &hdr, std::span<const std::byte> inner, const node_id &source)
{
    const bool has_source_identity = (hdr.flags & wire::k_flag_source_identity) != 0;
    if(s.m_on_message_with_info_cb)
    {
        const message_info info = assemble_message_info(s, hdr);
        s.m_messages.deliver(s.m_msg_peer, inner, info, source, has_source_identity,
                             [&s](std::string_view fqn, std::span<const std::byte> data, const message_info &mi) { s.m_on_message_with_info_cb(fqn, data, mi); });
        return;
    }
    if(s.m_on_message_cb)
    {
        s.m_messages.deliver(s.m_msg_peer, inner, source, has_source_identity, [&s](std::string_view fqn, std::span<const std::byte> data) { s.m_on_message_cb(fqn, data); });
        return;
    }
    if(s.m_on_message_route_cb)
    {
        const message_info info = assemble_message_info(s, hdr);
        s.m_messages.deliver(s.m_msg_peer, inner, info, source, has_source_identity,
                             [&s](std::string_view fqn, std::span<const std::byte> data, const message_info &mi) { s.m_on_message_route_cb(fqn, data, mi); });
        return;
    }
    s.m_messages.deliver(s.m_msg_peer, inner, source, has_source_identity, [](std::string_view, std::span<const std::byte>) {});
}

// The plain unidirectional path: pins the source to m_ctx.peer_id — the anti-spoofing invariant. An
// ordinary frame's delivered source is always the session peer, never a node_id lifted from the frame.
template<typename Session>
void deliver_session_data(Session &s, const wire::frame_header &hdr, std::span<const std::byte> inner)
{
    deliver_data_with_source(s, hdr, inner, s.m_ctx.peer_id);
}

// Every path releases the carrier reference the channel delivered: an unresolvable hash is
// warned and dropped but still released, never leaked.
template<typename Session>
void deliver_session_object(Session &s, const object_carrier &c)
{
    auto fqn = s.m_messages.fqn_for(c.topic_hash);
    if(fqn.empty())
    {
        s.m_logger.warn("plexus: peer_session object_topic_unknown");
        return release(c);
    }
    if(s.m_on_object_route_cb)
        s.m_on_object_route_cb(fqn, c);
    release(c);
}

}

#endif
