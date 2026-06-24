#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FANOUT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FANOUT_H

#include "plexus/io/qos_rxo.h"
#include "plexus/io/locality.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/observation_events.h"

#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"

#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

// The producer-side reaction to an arriving subscribe: match the subscriber's type_id, then run
// the full QoS compatibility gate. Each refusal replies a status, registers NO fan-out entry, and
// returns false (a per-topic refusal, never a peer teardown). Matching authority is the type_id
// alone (an equality compare, no parsing risk; subscribe.h caps the string fields). RELOCATION of
// the forwarder body — the forwarder still owns the registry/endpoint/global-default; this carries
// the gate out (it is a friend, so it reads them through the forwarder reference).
template<typename Forwarder, typename Peer>
// NOLINTNEXTLINE(readability-function-size)
bool attach_for_fanout(Forwarder &f, const Peer &p, std::string_view fqn, std::optional<std::uint64_t> subscriber_type_id, const subscriber_qos &sub_qos)
{
    auto hash = wire::fqn_topic_hash(fqn);
    if(f.type_id_mismatch(hash, subscriber_type_id))
    {
        auto resp = wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::type_mismatch});
        f.send_control(p.channel, wire::msg_type::subscribe_response, resp);
        return false;
    }
    // A typed-and-strict subscriber refuses an untyped producer. Ordered AFTER type_mismatch (a
    // declared-vs-declared mismatch is more specific).
    if(sub_qos.posture == attach_posture::strict && subscriber_type_id && !f.m_endpoint.registry().producer_type_id(hash))
    {
        auto resp = wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::type_undeclared});
        f.send_control(p.channel, wire::msg_type::subscribe_response, resp);
        return false;
    }
    // The request-vs-offered gate: the REQUESTED sub_qos arrived off the wire, the OFFERED qos +
    // source-identity offer are read locally.
    const topic_qos offered    = f.m_endpoint.registry().qos_for(hash);
    const bool      offers_sid = f.m_endpoint.registry().offers_source_identity(hash);
    const auto      rxo        = io::rxo_check(offered, offers_sid, f.m_global_default, sub_qos);
    if(rxo.verdict == io::rxo_verdict::incompatible_qos || rxo.verdict == io::rxo_verdict::source_identity_incompatible)
    {
        auto resp = wire::encode_subscribe_response({.topic_hash = hash, .status = Forwarder::status_of(rxo.verdict)});
        f.send_control(p.channel, wire::msg_type::subscribe_response, resp);
        f.emit_qos_change(qos_edge::refused, hash, sub_qos, rxo.verdict, subscriber_type_id);
        return false;
    }
    if(f.m_endpoint.registry().bump_refcount(p.node_name, fqn) != 1u)
        return false;
    f.m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name, sub_qos, subscriber_type_id);
    f.emit_qos_change(rxo.verdict == io::rxo_verdict::degraded ? qos_edge::degraded : qos_edge::accepted, hash, sub_qos, rxo.verdict, subscriber_type_id);
    // A degraded verdict ADMITS (the consumer chose permissive) but the reply carries the
    // degraded-field set so the accept is non-silent.
    auto resp = rxo.verdict == io::rxo_verdict::degraded
            ? wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::subscribed_degraded, .has_degraded = true, .degraded_flags = rxo.degraded_fields})
            : wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::subscribed});
    f.send_control(p.channel, wire::msg_type::subscribe_response, resp);
    f.replay_if_latched(p, hash);
    // A remote subscribe arrived — the local node is the PUBLISHER, so a co-host upgrade mints the
    // send companion the publish fan routes over.
    f.emit_demand_transition(p.node_name, fqn, demand_transition::up, demand_role::publisher);
    return true;
}

}

#endif
