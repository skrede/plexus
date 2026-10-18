#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONTROL_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CONTROL_H

#include "plexus/io/frame_router.h"
#include "plexus/io/subscribe_qos_wire.h"

#include "plexus/io/detail/peer_session_forward.h"
#include "plexus/io/detail/peer_report_consumers.h"
#include "plexus/io/detail/peer_session_subscribe.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/subscribe.h"
#include "plexus/wire/fetch_latched.h"
#include "plexus/wire/topic_declaration.h"

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

// The reporter is the handshake-proven sender, distinct from the report's origin.
template<typename Session, typename Forwarder>
void on_peer_report_received(Session &s, Forwarder &messages, std::span<const std::byte> inner)
{
    if(s.is_complete())
        ingest_peer_report_frame(messages, s.peer_identity(), inner);
}

// The sending relay is the proven identity the admission gate keys on, distinct from the trailer
// origin the inner frame is delivered as coming from.
template<typename Session>
void on_forwarded_received(Session &s, std::span<const std::byte> inner)
{
    if(s.is_complete())
        deliver_forwarded_frame(s, s.peer_identity(), inner);
}

template<typename Session, typename Forwarder>
void on_fetch_latched_received(Session &s, Forwarder &messages, std::span<const std::byte> inner)
{
    if(auto req = wire::decode_fetch_latched_request(inner))
        messages.fetch_latched(s.msg_peer(), req->topic_hash, req->max_samples);
}

template<typename Session, typename Forwarder>
void register_control_consumers(Session &s, frame_router &router, Forwarder &messages)
{
    router.on_subscribe([&s](std::span<const std::byte> inner) { on_subscribe_received(s, inner); });
    router.on_declare([&s](std::span<const std::byte> inner) { on_declare_received(s, inner); });
    router.on_peer_report([&s, &messages](std::span<const std::byte> inner) { on_peer_report_received(s, messages, inner); });
    router.on_forwarded([&s](std::span<const std::byte> inner) { on_forwarded_received(s, inner); });
    router.on_fetch_latched([&s, &messages](std::span<const std::byte> inner) { on_fetch_latched_received(s, messages, inner); });
    router.on_subscribe_response([&s](std::span<const std::byte> inner) { on_subscribe_response_received(s, inner); });
}

}

#endif
