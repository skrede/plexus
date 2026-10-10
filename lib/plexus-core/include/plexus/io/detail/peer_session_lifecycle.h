#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_LIFECYCLE_H

#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <system_error>

namespace plexus::io::detail {

template<typename Session>
void tear_down_session(Session &s)
{
    const bool was_complete = s.m_forwarders_installed;
    s.m_torn_down           = true;
    s.m_handshake_timer.cancel();
    s.m_messages.detach_all(s.m_msg_peer);
    s.m_procedures.detach_all(s.m_rpc_peer);
    s.m_peer_session_id          = 0;
    s.m_forwarders_installed     = false;
    s.m_outstanding_subscribes   = 0;
    s.m_ready_latched_this_cycle = false;
    s.m_fsm.on_torn_down();
    s.m_channel.close();
    if(was_complete)
        detail::fire_lifecycle(s, lifecycle_edge::disconnected);
}

template<typename Session>
void close_for_protocol_error(Session &s, wire::close_cause)
{
    if(s.m_torn_down)
        return;
    s.m_closed_for_protocol_error = true;
    s.m_logger.warn("plexus: peer_session protocol_close");
    tear_down_session(s);
}

template<typename Session>
void protocol_close(Session &s, wire::close_cause cause)
{
    if(s.m_torn_down)
        return;
    if(s.m_negotiator.engaged() && s.m_forwarders_installed)
        detail::fire_security(s, security_kind::stream_tamper_teardown, security_cause::tag_verify_failed);
    close_for_protocol_error(s, cause);
}

// The staleness gate runs before the router: a frame whose non-zero session_id differs from
// the latched epoch is a previous-session straggler and is dropped; the first non-zero
// observation latches the peer's epoch.
template<typename Session>
void receive_frame(Session &s, std::span<const std::byte> frame)
{
    if(s.m_torn_down)
        return;
    auto hdr = wire::decode_header(frame);
    if(!hdr)
        return close_for_protocol_error(s, wire::close_cause::invalid_magic);
    if(hdr->session_id != 0 && s.m_peer_session_id != 0 && hdr->session_id != s.m_peer_session_id)
        return;
    if(hdr->session_id != 0 && s.m_peer_session_id == 0)
        s.m_peer_session_id = hdr->session_id;
    s.m_router.route(frame);
}

template<typename Session>
void arm_handshake_timer(Session &s)
{
    s.m_handshake_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(s.m_handshake_timeout));
    s.m_handshake_timer.async_wait(
            [&s](std::error_code ec)
            {
                if(ec)
                    return;
                const auto step = s.m_fsm.on_timeout();
                detail::execute(s, step);
                // A bounded handshake re-send keeps the window open: re-arm here so a subsequent
                // loss is also bounded. arm_handshake_timer stays the single source of the window.
                if(step.action == fsm_action::send_request)
                    arm_handshake_timer(s);
            });
}

}

#endif
