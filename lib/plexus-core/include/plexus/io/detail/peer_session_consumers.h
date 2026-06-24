#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONSUMERS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONSUMERS_H

#include "plexus/io/subscribe_qos_wire.h"

#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/io/security/attach_facts.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/fetch_latched.h"

#include <span>
#include <cstddef>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

template<typename Session>
// NOLINTNEXTLINE(readability-function-size)
void register_session_consumers(Session &s)
{
    s.m_router.on_handshake_req(
            [&s](std::span<const std::byte> inner)
            {
                if(auto req = wire::decode_handshake_request(inner))
                {
                    s.m_negotiator.assemble(*req, security::attach_role::responder, req->id, s.m_fsm_cfg.self_id, s.m_fsm.attach_policy() != nullptr);
                    s.m_negotiator.latch_peer_proof(*req);
                    if(s.m_negotiator.posture_mismatched(*req))
                        return refuse_posture(s);
                    execute(s, s.m_fsm.on_request(*req, s.m_is_inbound_bootstrap, s.m_negotiator.facts()));
                }
            });
    s.m_router.on_handshake_resp(
            [&s](std::span<const std::byte> inner)
            {
                if(auto resp = wire::decode_handshake_response(inner))
                {
                    s.m_negotiator.assemble(*resp, security::attach_role::initiator, resp->id, s.m_fsm_cfg.self_id, s.m_fsm.attach_policy() != nullptr);
                    s.m_negotiator.latch_peer_proof(*resp);
                    if(s.m_negotiator.posture_mismatched(*resp))
                        return refuse_posture(s);
                    execute(s, s.m_fsm.on_response(*resp, s.m_negotiator.facts()));
                }
            });
    s.m_router.on_subscribe(
            [&s](std::span<const std::byte> inner)
            {
                if(auto req = wire::decode_subscribe_request(inner))
                {
                    // type_hash == 0 is the undeclared-type sentinel: lift it to nullopt so an
                    // undeclared subscriber is never refused.
                    std::optional<std::uint64_t> subscriber_type_id;
                    if(req->type_hash != 0)
                        subscriber_type_id = req->type_hash;
                    const subscriber_qos sub_qos = req->has_qos ? from_wire_region(req->qos) : subscriber_qos{};
                    s.m_messages.attach_for_fanout(s.m_msg_peer, req->fqn, subscriber_type_id, sub_qos);
                }
            });
    s.m_router.on_fetch_latched(
            [&s](std::span<const std::byte> inner)
            {
                if(auto req = wire::decode_fetch_latched_request(inner))
                    s.m_messages.fetch_latched(s.m_msg_peer, req->topic_hash, req->max_samples);
            });
    s.m_router.on_subscribe_response([&s](std::span<const std::byte> inner) { on_subscribe_response_received(s, inner); });
    s.m_router.on_unidirectional([&s](const wire::frame_header &hdr, std::span<const std::byte> inner) { deliver_session_data(s, hdr, inner); });
    s.m_router.on_rpc_request([&s](std::span<const std::byte> inner) { s.m_procedures.deliver_request(s.m_rpc_peer, inner, s.m_session_id); });
    s.m_router.on_rpc_response([&s](std::span<const std::byte> inner) { s.m_procedures.deliver_response(s.m_rpc_peer, inner); });
    // A heartbeat stamps this pinned peer's last-seen only — presence, never a topic deadline.
    s.m_router.on_heartbeat(
            [&s](std::span<const std::byte> inner)
            {
                if(wire::decode_heartbeat(inner) && s.m_on_stamp_seen)
                    s.m_on_stamp_seen(s.m_ctx.peer_id);
            });
}

}

#endif
