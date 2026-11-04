#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_DISCOVERY_GLUE_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_DISCOVERY_GLUE_H

#include "plexus/detail/address_parse.h"

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/locality.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/reliability_requirement.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <string>
#include <algorithm>

namespace plexus::detail {

// Direct awareness entries in the awareness table's order, then the keys of complete sessions that have no direct entry,
// in the session table's order. Caller target resolution takes the first of these with a complete session. A
// transitively reported origin is never a target.
template<typename Node, typename Fn>
void for_each_demand_target(Node &n, Fn &&fn)
{
    const auto &known = n.m_engine.known();
    known.for_each_candidate(
            [&](const plexus::node_id &id, const auto &reach, const auto &)
            {
                if(!reach.via)
                    fn(id);
            });
    n.m_engine.registry().for_each_connected(
            [&](const plexus::node_id &id, const auto &)
            {
                if(std::ranges::none_of(known.candidates(id), [](const io::route_candidate &c) { return c.is_direct(); }))
                    fn(id);
            });
}

// Idempotent per (peer, fqn).
template<typename Node>
void fan_demands_to(Node &n, const plexus::node_id &id)
{
    for(const auto &[rid, sub] : n.m_subscriptions.entries())
        n.m_engine.subscribe(id, sub.fqn, sub.qos, io::locality::any, io::reliability_requirement::any, sub.type_id, sub.type_name);
}

// The host is empty until the first listen. A real mDNS backend fills it from the resolved
// record; over static_discovery the node supplies it.
template<typename Node>
void advertise_card(Node &n)
{
    n.m_disc.advertise({n.m_service_name, io::endpoint{"", n.m_host}, discovery::assemble_contact_card(n.m_id, n.m_listens)});
}

// The card is untrusted multicast input: a malformed/missing node_id, a self card, a host-less
// address, or no usable port key each abort without a note_peer. The first valid port key wins.
template<typename Node>
void note_from_card(Node &n, const discovery::service_info &peer)
{
    const auto peer_id = plexus::detail::card_node_id(peer.metadata);
    if(!peer_id || *peer_id == n.m_id)
        return;
    const std::string host = plexus::detail::host_of(peer.endpoint.address);
    if(host.empty())
        return;
    if(const auto ep = plexus::detail::first_port_endpoint(peer.metadata, host))
    {
        n.m_engine.note_peer(*peer_id, *ep);
        fan_demands_to(n, *peer_id);
    }
}

// A goodbye removes awareness only, mirroring note_from_card's id/self guards; an unknown or self id is a harmless no-op.
template<typename Node>
void forget_from_card(Node &n, const discovery::service_info &peer)
{
    const auto peer_id = plexus::detail::card_node_id(peer.metadata);
    if(!peer_id || *peer_id == n.m_id)
        return;
    n.m_engine.forget(*peer_id);
}

}

#endif
