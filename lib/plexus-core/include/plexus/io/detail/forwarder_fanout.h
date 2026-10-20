#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FANOUT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FANOUT_H

#include "plexus/io/qos_rxo.h"
#include "plexus/io/locality.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/observation_events.h"

#include "plexus/io/detail/forwarder_publish.h"
#include "plexus/io/detail/forwarder_companion.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

inline wire::subscribe_status status_of(io::rxo_verdict v)
{
    return v == io::rxo_verdict::source_identity_incompatible ? wire::subscribe_status::source_identity_incompatible : wire::subscribe_status::incompatible_qos;
}

// A degraded verdict admits but carries the degraded-field set, so the accept is non-silent.
inline std::vector<std::byte> accepted_response(std::uint64_t hash, const io::rxo_result &rxo, companion_event offer)
{
    if(rxo.verdict != io::rxo_verdict::degraded)
        return wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::subscribed, .offer = offer_state_of(offer)});
    return wire::encode_subscribe_response(
            {.topic_hash = hash, .status = wire::subscribe_status::subscribed_degraded, .offer = offer_state_of(offer), .has_degraded = true, .degraded_flags = rxo.degraded_fields});
}

// Each refusal replies a status, registers no fan-out entry, and returns false — a per-topic
// refusal, never a peer teardown.
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
    // Ordered after type_mismatch: a declared-vs-declared mismatch is the more specific refusal.
    if(sub_qos.posture == attach_posture::strict && subscriber_type_id && !f.m_endpoint.registry().producer_type_id(hash))
    {
        auto resp = wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::type_undeclared});
        f.send_control(p.channel, wire::msg_type::subscribe_response, resp);
        return false;
    }
    const topic_qos offered = f.m_endpoint.registry().qos_for(hash);
    const bool offers_sid   = f.m_endpoint.registry().offers_source_identity(hash);
    const auto rxo          = io::rxo_check(offered, offers_sid, f.m_global_default, sub_qos);
    if(rxo.verdict == io::rxo_verdict::incompatible_qos || rxo.verdict == io::rxo_verdict::source_identity_incompatible)
    {
        auto resp = wire::encode_subscribe_response({.topic_hash = hash, .status = status_of(rxo.verdict)});
        f.send_control(p.channel, wire::msg_type::subscribe_response, resp);
        f.emit_qos_change(qos_edge::refused, hash, sub_qos, rxo.verdict, subscriber_type_id);
        return false;
    }
    if(!f.m_endpoint.admit_gate(p.node_name, fqn))
        return false;
    f.m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name, sub_qos, subscriber_type_id);
    f.emit_qos_change(rxo.verdict == io::rxo_verdict::degraded ? qos_edge::degraded : qos_edge::accepted, hash, sub_qos, rxo.verdict, subscriber_type_id);
    // The up-edge mints and stamps the ring before the response can tell the subscriber it exists.
    f.emit_demand_transition(p.node_name, fqn, demand_transition::up, demand_role::publisher);
    f.send_control(p.channel, wire::msg_type::subscribe_response, accepted_response(hash, rxo, offer_of(f.m_companion_hooks, p.node_name, fqn)));
    replay_if_latched(f, p, hash);
    return true;
}

}

#endif
