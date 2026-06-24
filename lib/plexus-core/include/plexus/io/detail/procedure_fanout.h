#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_FANOUT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PROCEDURE_FANOUT_H

#include "plexus/io/observation_events.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <cstdint>
#include <string_view>

namespace plexus::io::detail {

// Forward-declared because reply_status() calls it before its definition below.
template<typename Forwarder, typename Channel>
void send_data(Forwarder &f, Channel &channel, wire::msg_type type, std::span<const std::byte> inner, std::uint64_t session_id);

// The rpc observation fan: hand the call/serve/reply edge to the engine sink (which posts a
// snapshot to the observer fan-out); absent = one branch. The surfaced view is envelope-only (the
// correlation_id, and status for a reply) — the param/return spans are transient, so no
// borrowed-span dangles into the deferred turn and no per-call copy is imposed. RELOCATION of the
// forwarder body — the sinks live on the forwarder; these read them through a friend reference.

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

// reply_status: frame an rpc_response carrying the request's correlation_id back to the peer
// (source = procedure) and send it through reused scratch. The correlation_id alone matches the
// response to its pending request; the type tag words are reserved zeroes carrying no type-matching
// authority (matching is subscribe-time discovery), so the response writes them 0 rather than
// echoing.
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

// send_data: emit a DATA frame (rpc_request / rpc_response) carrying the per-send session_id epoch
// so the receive-side staleness gate can fire. Absence keeps the unestablished sentinel 0 — the
// epoch is per-peer, passed per send (a node-shared forwarder fans to many peers), never a
// forwarder-wide member.
template<typename Forwarder, typename Channel>
void send_data(Forwarder &f, Channel &channel, wire::msg_type type, std::span<const std::byte> inner, std::uint64_t session_id)
{
    wire::frame_header fhdr{.type = type, .flags = 0, .session_id = session_id, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = inner.size()};
    wire::encode_frame_into(f.m_frame_scratch, fhdr, inner);
    channel.send(f.m_frame_scratch);
}

// send_subscribe: emit the procedure subscribe (a CONTROL frame, session-pre) for an attach.
template<typename Forwarder, typename Channel>
void send_subscribe(Forwarder &f, Channel &channel, std::string_view fqn, std::uint64_t hash)
{
    wire::subscribe_request req{.fqn = std::string{fqn}, .type_name = {}, .topic_hash = hash, .type_hash = 0, .source = wire::endpoint_source_type::caller};
    f.m_endpoint.send_subscribe(channel, req);
}

}

#endif
