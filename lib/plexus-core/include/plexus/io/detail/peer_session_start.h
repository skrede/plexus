#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_START_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_START_H

#include "plexus/io/io_error.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/disconnect_cause.h"

#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_consumers.h"
#include "plexus/io/detail/peer_session_lifecycle.h"

#include "plexus/wire/close_cause.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

template<typename Session>
void wire_channel_seams(Session &s)
{
    using channel_type = typename Session::channel_type;

    s.m_channel.on_data([&s](std::span<const std::byte> f) { detail::receive_frame(s, f); });
    s.m_channel.on_error(
            [&s](io_error e)
            {
                // Both are refused sends that leave the channel open; only a genuine channel break drives the
                // transport-drop reconnect.
                if(e == io_error::would_block || e == io_error::message_too_large)
                    return;
                if(s.m_on_drop_cb && !s.m_torn_down && !s.m_closed_for_protocol_error)
                    s.m_on_drop_cb();
            });
    s.m_channel.on_protocol_close([&s](wire::close_cause cause) { detail::protocol_close(s, cause); });
    if constexpr(requires(channel_type &c) { c.on_object([](const object_carrier &) {}); })
        s.m_channel.on_object([&s](const object_carrier &c) { detail::deliver_session_object(s, c); });
}

template<typename Session>
void start_session(Session &s)
{
    s.m_torn_down                 = false;
    s.m_quiet_teardown            = false;
    s.m_closed_for_protocol_error = false;
    s.m_disconnect_cause          = disconnect_cause::unspecified;
    s.m_unsolicited.clear();
    s.m_negotiator.prime();
    wire_channel_seams(s);
    detail::register_session_consumers(s);
    detail::arm_handshake_timer(s);
    if(!s.m_is_inbound_bootstrap)
    {
        detail::execute(s, s.m_fsm.on_dial_started());
        detail::execute(s, s.m_fsm.on_outbound_connected());
    }
}

}

#endif
