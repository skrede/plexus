#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONSUMERS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONSUMERS_H

#include "plexus/io/subscribe_qos_wire.h"

#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_forward.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_originate.h"
#include "plexus/io/detail/peer_report_consumers.h"

#include "plexus/io/security/attach_facts.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_declaration.h"
#include "plexus/wire/fetch_latched.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

// An unauthenticated peer must not reach the graph: only a complete session's records are folded,
// so a record cannot be injected before the handshake settles who is speaking.
//
// The edge is attributed to the identity the handshake proved, NOT to the slot's key: an accepted
// session's slot carries a provisional id minted before the peer spoke, and a pair that both dials
// and accepts holds two sessions. Keying on the proven identity collapses both onto the one
// participant the awareness table also names, so an edge is never attributed to a peer that no
// enumeration lists.
template<typename Session>
void fold_topic_edge(Session &s, std::string_view topic, std::string_view type_name, std::optional<std::uint64_t> type_id, graph::topic_role role)
{
    if(!s.is_complete())
        return;
    s.m_messages.note_topic_edge(graph::topic_edge{s.m_fsm.last_seen_peer_id(), topic, type_name, type_id, role});
}

// The three wire states collapse to the schema's reserved optional: no assertion is nullopt, and a
// declared type keeps its id whatever the name — an empty name is a type declared without one.
inline std::optional<std::uint64_t> declared_id(const wire::topic_declaration &td)
{
    return td.state == wire::type_state::undeclared ? std::nullopt : std::optional{td.type_id};
}

// The declared flag, not the id, gates the graph: the type assertion is independent of the numeric
// token the fan-out matches on.
inline std::optional<std::uint64_t> declared_id(const wire::subscribe_request &req)
{
    return req.type_declared ? std::optional{req.type_hash} : std::nullopt;
}

template<typename Session>
void on_subscribe_received(Session &s, std::span<const std::byte> inner)
{
    auto req = wire::decode_subscribe_request(inner);
    if(!req)
        return;
    // type_hash == 0 is the undeclared-type sentinel: lift it to nullopt so an undeclared
    // subscriber is never refused.
    std::optional<std::uint64_t> subscriber_type_id;
    if(req->type_hash != 0)
        subscriber_type_id = req->type_hash;
    const subscriber_qos sub_qos = req->has_qos ? from_wire_region(req->qos) : subscriber_qos{};
    s.m_messages.attach_for_fanout(s.m_msg_peer, req->fqn, subscriber_type_id, sub_qos);
    fold_topic_edge(s, req->fqn, req->type_name, declared_id(*req), graph::topic_role::subscriber);
}

template<typename Session>
void on_declare_received(Session &s, std::span<const std::byte> inner)
{
    if(auto td = wire::decode_topic_declaration(inner))
        fold_topic_edge(s, td->fqn, td->type_name, declared_id(*td), graph::topic_role::publisher);
}

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
    s.m_router.on_subscribe([&s](std::span<const std::byte> inner) { on_subscribe_received(s, inner); });
    s.m_router.on_declare([&s](std::span<const std::byte> inner) { on_declare_received(s, inner); });
    // The always-compiled peer_report receive half: a complete session's 0x0E frame decodes and folds
    // through the shared message seam into the engine gate chain, attributed to the handshake-proven
    // sender as the reporter (distinct from the report's origin).
    s.m_router.on_peer_report(
            [&s](std::span<const std::byte> inner)
            {
                if(s.is_complete())
                    ingest_peer_report_frame(s.m_messages, s.m_fsm.last_seen_peer_id(), inner);
            });
    // The always-compiled forwarded receive half: a complete session's 0x0F frame runs the pure
    // admission gate (self/loop, hop, (origin, arrival-relay) dedup) and, on admit, delivers the inner
    // frame with the delivery-edge origin override. The SENDING relay is attributed as the proven
    // identity for gate bookkeeping, distinct from the trailer origin delivered as the message source.
    s.m_router.on_forwarded(
            [&s](std::span<const std::byte> inner)
            {
                if(s.is_complete())
                    deliver_forwarded_frame(s, s.m_fsm.last_seen_peer_id(), inner);
            });
    s.m_router.on_fetch_latched(
            [&s](std::span<const std::byte> inner)
            {
                if(auto req = wire::decode_fetch_latched_request(inner))
                    s.m_messages.fetch_latched(s.m_msg_peer, req->topic_hash, req->max_samples);
            });
    s.m_router.on_subscribe_response([&s](std::span<const std::byte> inner) { on_subscribe_response_received(s, inner); });
    // The plain unidirectional receive: deliver locally, then (relay-only) re-originate as a forwarded
    // envelope so a directly-attached publisher's publish transits onward to remote subscribers.
    s.m_router.on_unidirectional(
            [&s](const wire::frame_header &hdr, std::span<const std::byte> inner)
            {
                deliver_session_data(s, hdr, inner);
                originate_if_pubsub(s, hdr, inner);
            });
    s.m_router.on_rpc_request([&s](std::span<const std::byte> inner) { s.m_procedures.deliver_request(s.m_rpc_peer, inner, s.m_session_id); });
    s.m_router.on_rpc_response([&s](std::span<const std::byte> inner) { s.m_procedures.deliver_response(s.m_rpc_peer, inner); });
    // A heartbeat stamps this pinned peer's last-seen only — presence, never a topic deadline.
    s.m_router.on_heartbeat(
            [&s](std::span<const std::byte> inner)
            {
                const auto hb = wire::decode_heartbeat(inner);
                if(!hb)
                    return;
                if(s.m_on_stamp_seen_cb)
                    s.m_on_stamp_seen_cb(s.m_ctx.peer_id);
                // The relay honor points key on the handshake-proven identity (m_reported, the broadcast
                // self-skip), not the provisional context id a dialed/accepted slot carries.
                if(s.m_on_decline_seen_cb)
                    s.m_on_decline_seen_cb(s.peer_identity(), (hb->reserved & wire::k_heartbeat_relay_decline_flag) != 0);
            });
}

}

#endif
