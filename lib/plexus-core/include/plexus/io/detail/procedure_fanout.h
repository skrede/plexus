#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_FANOUT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_FANOUT_H

#include "plexus/io/observation_events.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <cstdint>
#include <string_view>

namespace plexus::io::detail {

// Forward-declared: reply_status() calls it before its definition below.
template<typename Forwarder, typename Channel>
void send_data(Forwarder &f, Channel &channel, wire::msg_type type, std::span<const std::byte> inner, std::uint64_t session_id);

// The surfaced view is envelope-only (correlation_id, plus status for a reply): the param/return
// spans are transient, so no borrowed span dangles into the deferred turn.
template<typename Forwarder>
void emit_rpc_call(Forwarder &f, std::string_view fqn, std::uint64_t corr_id)
{
    if(f.m_on_rpc_call)
        f.m_on_rpc_call(fqn, rpc_view{.correlation_id = corr_id});
}

template<typename Forwarder>
void emit_rpc_serve(Forwarder &f, std::string_view fqn, std::uint64_t corr_id)
{
    if(f.m_on_rpc_serve)
        f.m_on_rpc_serve(fqn, rpc_view{.correlation_id = corr_id});
}

template<typename Forwarder>
void emit_rpc_reply(Forwarder &f, std::string_view fqn, std::uint64_t corr_id, wire::rpc_status status)
{
    if(f.m_on_rpc_reply)
        f.m_on_rpc_reply(fqn, rpc_reply_view{.correlation_id = corr_id, .status = status});
}

// correlation_id alone matches the response to its pending request; the type-tag words carry no
// matching authority (matching is subscribe-time discovery), so the response writes them 0.
template<typename Forwarder, typename Channel>
void reply_status(Forwarder &f, Channel &channel, const wire::bidirectional_header &req_hdr, wire::rpc_status status, std::span<const std::byte> return_data, std::uint64_t session_id)
{
    wire::bidirectional_header resp_hdr{.source         = wire::endpoint_source_type::procedure,
                                        .sequence       = f.m_endpoint.next_sequence(),
                                        .topic_hash     = req_hdr.topic_hash,
                                        .type_hash_1    = 0,
                                        .type_hash_2    = 0,
                                        .correlation_id = req_hdr.correlation_id};
    wire::encode_rpc_response_into(f.m_resp_scratch, resp_hdr, status, return_data);
    send_data(f, channel, wire::msg_type::rpc_response, f.m_resp_scratch, session_id);
}

// The session_id epoch is per-peer, passed per send (a node-shared forwarder fans to many
// peers), never a forwarder-wide member; 0 is the unestablished sentinel.
template<typename Forwarder, typename Channel>
void send_data(Forwarder &f, Channel &channel, wire::msg_type type, std::span<const std::byte> inner, std::uint64_t session_id)
{
    wire::frame_header fhdr{.type = type, .flags = 0, .session_id = session_id, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = inner.size()};
    wire::encode_frame_into(f.m_frame_scratch, fhdr, inner);
    channel.send(f.m_frame_scratch);
}

template<typename Forwarder, typename Channel>
void send_subscribe(Forwarder &f, Channel &channel, std::string_view fqn, std::uint64_t hash)
{
    wire::subscribe_request req{.fqn = std::string{fqn}, .type_name = {}, .topic_hash = hash, .type_hash = 0, .source = wire::endpoint_source_type::caller};
    f.m_endpoint.send_subscribe(channel, req);
}

}

#endif
