#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_DEMAND_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_DEMAND_H

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/observation_events.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"

#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io {

struct remembered_demand
{
    std::string fqn;
    subscriber_qos qos;
    std::optional<std::uint64_t> type_id; // nullopt = undeclared
    // Owned, not borrowed: a replay on reconnect outlives whatever named the type at attach.
    std::string type_name;
};

namespace detail {

template<typename Forwarder>
void record_remote_topic(Forwarder &f, const std::string &node_name, std::string_view fqn, const subscriber_qos &qos, std::optional<std::uint64_t> type_id = std::nullopt,
                         std::string_view type_name = {})
{
    auto &topics = f.m_remote_topics[node_name];
    for(const auto &existing : topics)
        if(existing.fqn == fqn)
            return; // idempotent: keep the first stored qos + type
    topics.emplace_back(remembered_demand{std::string{fqn}, qos, type_id, std::string{type_name}});
}

template<typename Forwarder>
void forget_remote_topic(Forwarder &f, const std::string &node_name, std::string_view fqn)
{
    auto it = f.m_remote_topics.find(node_name);
    if(it == f.m_remote_topics.end())
        return;
    std::erase_if(it->second, [&](const remembered_demand &d) { return d.fqn == fqn; });
    if(it->second.empty())
        f.m_remote_topics.erase(it);
}

template<typename Forwarder>
const std::vector<remembered_demand> &remembered_topics(const Forwarder &f, const std::string &node_name)
{
    auto it = f.m_remote_topics.find(node_name);
    return it == f.m_remote_topics.end() ? f.m_empty : it->second;
}

template<typename Forwarder, typename Peer>
bool forwarder_detach(Forwarder &f, const Peer &p, std::string_view fqn)
{
    if(!f.m_endpoint.detach_gate(p.node_name, fqn))
        return false;

    auto hash = wire::fqn_topic_hash(fqn);
    f.m_endpoint.registry().remove_subscriber(hash, p.channel);
    f.m_egress.remove(p.channel);
    forget_remote_topic(f, p.node_name, fqn);

    auto req = wire::encode_unsubscribe_request({.topic_hash = hash});
    f.send_control(p.channel, wire::msg_type::unsubscribe, req);
    f.emit_qos_change(qos_edge::unsubscribed, hash, subscriber_qos{}, rxo_verdict::compatible, std::nullopt);
    f.emit_demand_transition(p.node_name, fqn, demand_transition::down, demand_role::subscriber);
    return true;
}

template<typename Forwarder, typename Peer>
void forwarder_detach_all(Forwarder &f, const Peer &p)
{
    f.m_endpoint.remove_peer(p);
    f.m_egress.remove(p.channel);
}

template<typename Forwarder, typename Channel>
void forwarder_detach_local(Forwarder &f, std::string_view fqn, Channel &channel)
{
    f.m_endpoint.registry().remove_subscriber(wire::fqn_topic_hash(fqn), channel);
    f.m_egress.remove(channel);
    auto it = f.m_self_route_channels.find(static_cast<const void *>(&channel));
    if(it != f.m_self_route_channels.end() && --it->second == 0)
        f.m_self_route_channels.erase(it);
}

}

}

#endif
