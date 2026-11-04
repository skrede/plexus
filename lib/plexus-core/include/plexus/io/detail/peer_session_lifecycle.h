#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_LIFECYCLE_H

#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/peer_session_gate.h"
#include "plexus/io/detail/peer_session_retry.h"
#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/wire/frame.h"
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
    s.m_session_timer.cancel();
    s.m_waits.close();
    s.m_messages.detach_all(s.m_msg_peer);
    s.m_procedures.detach_all(s.m_rpc_peer);
    s.m_peer_session_id          = 0;
    s.m_forwarders_installed     = false;
    s.m_outstanding_subscribes   = 0;
    s.m_ready_latched_this_cycle = false;
    detail::reset_retry_cycle(s.m_retry);
    s.m_fsm.on_torn_down();
    s.m_channel.close();
    if(was_complete && !s.m_quiet_teardown)
        detail::fire_lifecycle(s, lifecycle_edge::disconnected);
}

template<typename Session>
void quiet_tear_down(Session &s)
{
    s.m_quiet_teardown = true;
    tear_down_session(s);
}

template<typename Session>
bool is_demoted(const Session &s) noexcept
{
    return s.m_quiet_teardown;
}

// The close waits one handshake window, within which the remote demotes its own end, so it never crosses the remote's survivor completion.
template<typename Session>
void demote_loser(Session &s)
{
    s.m_messages.detach_all(s.m_msg_peer);
    s.m_procedures.detach_all(s.m_rpc_peer);
    s.m_forwarders_installed   = false;
    s.m_outstanding_subscribes = 0;
    s.m_quiet_teardown         = true;
    detail::reset_retry_cycle(s.m_retry);
    s.m_session_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(s.m_handshake_timeout));
    s.m_waits.begin();
    s.m_session_timer.async_wait(
            [&s](std::error_code ec)
            {
                s.m_waits.end();
                if(!ec && !s.m_torn_down)
                    quiet_tear_down(s);
            });
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

template<typename Session>
void note_admitted(Session &s, const wire::frame_header &hdr)
{
    if(!s.is_complete())
        return;
    if(hdr.session_id != 0 && s.m_peer_session_id == 0)
        s.m_peer_session_id = hdr.session_id;
    if(hdr.type != wire::msg_type::handshake_req && hdr.type != wire::msg_type::handshake_resp)
        detail::redeclare_once(s);
}

// The establishment gate runs first, so a refused frame can never latch an epoch. After it, a frame whose non-zero
// session_id differs from the latched epoch is a previous-session straggler; it is dropped and the session stays open.
template<typename Session>
void receive_frame(Session &s, std::span<const std::byte> frame)
{
    if(s.m_torn_down)
        return detail::report_drop(s, drop_cause::unestablished);
    auto hdr = wire::decode_header(frame);
    if(!hdr)
    {
        detail::report_malformed(s);
        return close_for_protocol_error(s, wire::close_cause::invalid_magic);
    }
    switch(detail::classify(s.is_complete(), hdr->type))
    {
        case gate_verdict::unestablished:
            return detail::report_drop(s, drop_cause::unestablished);
        case gate_verdict::malformed:
            return detail::report_malformed(s);
        case gate_verdict::admit:
            break;
    }
    if(hdr->session_id != 0 && s.m_peer_session_id != 0 && hdr->session_id != s.m_peer_session_id)
        return detail::report_drop(s, drop_cause::foreign_session);
    note_admitted(s, *hdr);
    s.m_router.route(frame);
}

template<typename Session>
void arm_handshake_timer(Session &s)
{
    s.m_session_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(s.m_handshake_timeout));
    s.m_waits.begin();
    s.m_session_timer.async_wait(
            [&s](std::error_code ec)
            {
                s.m_waits.end();
                if(ec || s.m_torn_down)
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
