#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_GATE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_GATE_H

#include "plexus/io/locality.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"

#include <chrono>
#include <cstdint>

namespace plexus::io::detail {

enum class gate_verdict : std::uint8_t
{
    admit,
    unestablished,
    malformed,
};

// decode_header casts the type byte unchecked, so a byte outside the enumeration reaches this switch. It has no
// default arm so that a new msg_type fails the build under -Wswitch until it is given a verdict here.
// NOLINTNEXTLINE(readability-function-size)
constexpr gate_verdict classify(bool complete, wire::msg_type type) noexcept
{
    switch(type)
    {
        case wire::msg_type::handshake_req:
        case wire::msg_type::handshake_resp:
            return gate_verdict::admit;
        case wire::msg_type::unidirectional:
        case wire::msg_type::subscribe:
        case wire::msg_type::unsubscribe:
        case wire::msg_type::fetch_latched:
        case wire::msg_type::rpc_request:
        case wire::msg_type::rpc_response:
        case wire::msg_type::subscribe_response:
        case wire::msg_type::heartbeat:
        case wire::msg_type::declare:
        case wire::msg_type::peer_report:
        case wire::msg_type::forwarded:
        case wire::msg_type::companion_ready:
        case wire::msg_type::companion_withdrawn:
        case wire::msg_type::companion_switch:
        case wire::msg_type::companion_offer:
            return complete ? gate_verdict::admit : gate_verdict::unestablished;
        // Neither side has a consumer for these, so they are malformed in every state.
        case wire::msg_type::bidirectional:
        case wire::msg_type::fetch_metadata:
            return gate_verdict::malformed;
    }
    return gate_verdict::malformed;
}

template<typename Session>
void report_drop(Session &s, drop_cause cause, std::uint64_t topic_hash = 0)
{
    if(s.m_on_frame_dropped_cb)
        s.m_on_frame_dropped_cb(drop_event{cause, tier_of(s.m_channel.remote_endpoint().scheme), 0, topic_hash, s.m_ctx.peer_id, 1});
}

template<typename Session>
void report_malformed(Session &s)
{
    report_drop(s, drop_cause::malformed);
    s.m_malformed_warning.note(s.m_logger, "plexus: peer_session malformed_frame", std::chrono::steady_clock::now());
}

}

#endif
