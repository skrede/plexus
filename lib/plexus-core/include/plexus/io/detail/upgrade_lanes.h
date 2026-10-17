#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_LANES_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_LANES_H

#include "plexus/io/detail/upgrade_rings.h"

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include <string>
#include <utility>
#include <string_view>

namespace plexus::io::detail {

// A declined gate keeps the wire receive path: nothing is retained.
template<typename Coordinator>
void lane_attach(Coordinator &c, std::string_view node_name, std::string_view fqn)
{
    if(!c.m_receive_mint)
        return;
    upgrade_receive received = c.m_receive_mint(node_name, fqn);
    if(!received.engaged())
        return;
    typename Coordinator::ring r;
    r.fqn     = std::string{fqn};
    r.receive = std::move(received);
    c.m_held[std::string{node_name}].push_back(std::move(r));
}

// A declined gate keeps the wire send path: nothing is retained.
template<typename Coordinator>
void lane_mint(Coordinator &c, std::string_view node_name, std::string_view fqn)
{
    if(!c.m_mint)
        return;
    auto minted = c.m_mint(fqn);
    if(!minted.channel)
        return;
    typename Coordinator::ring r;
    r.fqn     = std::string{fqn};
    r.channel = std::move(minted.channel);
    r.fits    = std::move(minted.fits);
    c.m_held[std::string{node_name}].push_back(std::move(r));
}

template<typename Coordinator>
void lane_up(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
{
    if(find_ring(c.m_held, node_name, fqn) != nullptr)
        return;
    if(!c.run_policy(c.m_registry.same_host_for(node_name), c.hint_for(fqn)))
        return;
    if(role == demand_role::subscriber)
        lane_attach(c, node_name, fqn);
    else
        lane_mint(c, node_name, fqn);
}

// The last demand for the fqn gone drops its hint, so a churn of distinct topic names cannot grow the hint table without
// bound (a re-subscribe re-records it from the declare path).
template<typename Coordinator>
void lane_down(Coordinator &c, std::string_view node_name, std::string_view fqn)
{
    erase_ring(c.m_held, node_name, fqn);
    if(!any_holder(c.m_held, fqn))
        c.m_hints.erase(std::string{fqn});
}

}

#endif
