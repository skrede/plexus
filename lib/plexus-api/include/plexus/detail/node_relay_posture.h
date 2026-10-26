#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_RELAY_POSTURE_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_RELAY_POSTURE_H

#include "plexus/io/route_options.h"
#include "plexus/io/peer_report_emitter.h"

#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"
#include "plexus/relay_posture.h"

#include <cstddef>
#include <type_traits>
#include <string_view>

namespace plexus::detail {

// The route count at which a leaf's via-relay reach is first disclosed: a value of 1
// discloses any relayed reach.
inline constexpr std::size_t k_relay_routes_disclosure_threshold = 1;

template<typename Node>
std::size_t count_routes_via_relays(const Node &n)
{
    std::size_t via = 0;
    n.m_engine.known().for_each_candidate([&](const plexus::node_id &id, const graph::route &reach, const graph::provenance &) {
        if(reach.via.has_value() && id != n.m_id)
            ++via;
    });
    return via;
}

template<typename Node>
relay_posture compute_relay_posture(const Node &n)
{
    relay_posture posture{};
    if constexpr(!std::is_same_v<typename Node::engine_type::peer_report_emitter_type, io::null_peer_report_emitter>)
    {
        posture.offering_relay   = true;
        posture.reported_origins = n.m_engine.reported_origin_count();
    }
    posture.routes_via_relays = count_routes_via_relays(n);
    posture.declining         = n.m_engine.route_opts().usage == io::route_usage::never;
    return posture;
}

inline std::string_view relay_posture_line(const relay_posture &posture) noexcept
{
    if(posture.offering_relay)
        return "plexus: node offers a cooperative relay path for its reported origins (self_check)";
    if(posture.declining)
        return "plexus: node cooperatively declines relayed use — a request to peers, not a security boundary (self_check)";
    return "plexus: node reaches one or more destinations via a relay (self_check)";
}

template<typename Node>
void disclose_relay_posture_once(const Node &n, const relay_posture &posture)
{
    if(n.m_relay_posture_warned)
        return;
    if(!posture.offering_relay && posture.routes_via_relays < k_relay_routes_disclosure_threshold && !posture.declining)
        return;
    n.m_logger.warn(relay_posture_line(posture));
    n.m_relay_posture_warned = true;
}

}

#endif
