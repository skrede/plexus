#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_OFFERS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_OFFERS_H

#include "plexus/io/detail/upgrade_hints.h"
#include "plexus/io/detail/upgrade_lanes.h"
#include "plexus/io/detail/upgrade_send_lanes.h"
#include "plexus/io/detail/upgrade_receive_lanes.h"

#include "plexus/io/companion_event.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include <set>
#include <string>
#include <vector>
#include <utility>
#include <functional>
#include <string_view>

namespace plexus::io::detail {

// The topics this node publishes, and every co-host peer whose subscribe to one of this node's topics was admitted,
// published or not, so a lane that appears later reaches the peers already answered.
class offer_table
{
public:
    bool published(std::string_view fqn) const
    {
        return m_published.contains(fqn);
    }

    void publish(std::string_view fqn, bool present)
    {
        if(present)
            m_published.emplace(fqn);
        else
            m_published.erase(std::string{fqn});
    }

    void remember(std::string_view node_name, std::string_view fqn)
    {
        m_demand.emplace(std::string{fqn}, std::string{node_name});
    }

    void forget(std::string_view node_name, std::string_view fqn)
    {
        m_demand.erase({std::string{fqn}, std::string{node_name}});
    }

    void forget_peer(std::string_view node_name)
    {
        std::erase_if(m_demand, [&](const auto &entry) { return entry.second == node_name; });
    }

    std::vector<std::string> peers_for(std::string_view fqn) const
    {
        std::vector<std::string> peers;
        for(auto it = m_demand.lower_bound({std::string{fqn}, std::string{}}); it != m_demand.end() && it->first == fqn; ++it)
            peers.push_back(it->second);
        return peers;
    }

private:
    std::set<std::string, std::less<>> m_published;
    std::set<std::pair<std::string, std::string>> m_demand;
};

// The coordinator's offer bodies, befriended as one. Only a node with a local publisher for a topic offers its lane. A
// peer the lane reaches after its subscribe response is offered it late, from a path outside its own up-edge, so no peer
// is offered twice.
struct upgrade_offers
{
    template<typename Coordinator>
    static void edge(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
    {
        if(dir == demand_transition::up)
            up(c, node_name, fqn, role);
        else
            down(c, node_name, fqn, role);
    }

    template<typename Coordinator>
    static void up(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
    {
        if(role == demand_role::publisher)
            c.m_offers.remember(node_name, fqn);
        if(role == demand_role::subscriber || c.m_offers.published(fqn))
            upgrade_lanes::up(c, node_name, fqn, role);
    }

    // A churn of distinct topic names cannot grow the hint table without bound; a topic published here keeps its
    // publisher's hint for its next subscriber.
    template<typename Coordinator>
    static void down(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
    {
        if(role == demand_role::publisher)
            c.m_offers.forget(node_name, fqn);
        upgrade_lanes::down(c, node_name, fqn, role);
        if(!c.m_offers.published(fqn) && !any_receive_lane(c.m_receive, fqn) && find_send_lane(c.m_send, fqn) == nullptr)
            upgrade_hints::forget(c, fqn);
    }

    // A retiring publisher tears down no lane, but a pending re-attempt would mint a ring for a topic it no longer has.
    template<typename Coordinator>
    static void publish(Coordinator &c, std::string_view fqn, bool present)
    {
        c.m_offers.publish(fqn, present);
        if(!present)
        {
            c.m_retry.take_waiting(fqn, demand_role::publisher);
            return;
        }
        for(const std::string &node_name : c.m_offers.peers_for(fqn))
            if(!offered(c.m_send, node_name, fqn))
                offer_late(c, node_name, fqn);
    }

    // A transient decline defers the peer, and the mint that later serves it offers it.
    template<typename Coordinator>
    static void offer_late(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        upgrade_lanes::attempt(c, node_name, fqn, demand_role::publisher);
        if(offered(c.m_send, node_name, fqn))
            upgrade_lanes::send(c, node_name, fqn, companion_event::offered, k_no_reader);
    }

    template<typename Coordinator>
    static companion_event offer_for(const Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        if(!c.m_offers.published(fqn))
            return companion_event::no_publisher;
        return offered(c.m_send, node_name, fqn) ? companion_event::offered : companion_event::not_offered;
    }

    template<typename Coordinator>
    static void forget_peer(Coordinator &c, std::string_view node_name)
    {
        c.m_offers.forget_peer(node_name);
        upgrade_send_lanes::forget_peer(c, node_name);
    }
};

}

#endif
