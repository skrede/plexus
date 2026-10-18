#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_LANES_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_LANES_H

#include "plexus/io/detail/upgrade_retry.h"
#include "plexus/io/detail/upgrade_rings.h"

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include <chrono>
#include <string>
#include <utility>
#include <string_view>

namespace plexus::io::detail {

// A declined gate keeps the wire receive path: nothing is retained.
template<typename Coordinator>
fallback_reason lane_attach(Coordinator &c, std::string_view node_name, std::string_view fqn)
{
    if(!c.m_receive_mint)
        return fallback_reason::none;
    upgrade_receive received = c.m_receive_mint(node_name, fqn);
    if(!received.engaged())
        return received.reason;
    typename Coordinator::ring r;
    r.fqn     = std::string{fqn};
    r.receive = std::move(received);
    c.m_held[std::string{node_name}].push_back(std::move(r));
    return fallback_reason::none;
}

// A declined gate keeps the wire send path: nothing is retained.
template<typename Coordinator>
fallback_reason lane_mint(Coordinator &c, std::string_view node_name, std::string_view fqn)
{
    if(!c.m_mint)
        return fallback_reason::none;
    auto minted = c.m_mint(fqn);
    if(!minted.channel)
        return minted.reason;
    typename Coordinator::ring r;
    r.fqn     = std::string{fqn};
    r.channel = std::move(minted.channel);
    r.fits    = std::move(minted.fits);
    c.m_held[std::string{node_name}].push_back(std::move(r));
    return fallback_reason::none;
}

// The reason a gate declined the lane; none when the lane engaged, is already held or the policy keeps the wire.
template<typename Coordinator>
fallback_reason lane_engage(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
{
    if(find_ring(c.m_held, node_name, fqn) != nullptr)
        return fallback_reason::none;
    if(!c.run_policy(c.m_registry.same_host_for(node_name), c.hint_for(fqn)))
        return fallback_reason::none;
    return role == demand_role::subscriber ? lane_attach(c, node_name, fqn) : lane_mint(c, node_name, fqn);
}

template<typename Coordinator>
void lane_arm(Coordinator &c)
{
    if(const std::chrono::milliseconds delay = c.m_retry.arm(); delay.count() != 0)
        c.m_schedule_retry_cb(delay);
}

// A transient decline leaves the pair on the wire and posts a re-attempt; it never waits for the creator here.
template<typename Coordinator>
void lane_up(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
{
    if(!retryable(lane_engage(c, node_name, fqn, role)) || !c.m_schedule_retry_cb)
        return;
    c.m_retry.defer(node_name, fqn, role);
    lane_arm(c);
}

// A gate may tear a pair down while this wake runs, so each due pair is re-checked before its re-attempt.
template<typename Coordinator>
void lane_retry(Coordinator &c)
{
    for(const retry_key &key : c.m_retry.take_due())
        if(c.m_retry.contains(key))
            c.m_retry.settle(key, lane_engage(c, key.node_name, key.fqn, key.role));
    lane_arm(c);
}

// The last demand for the fqn gone drops its hint, so a churn of distinct topic names cannot grow the hint table without
// bound (a re-subscribe re-records it from the declare path).
template<typename Coordinator>
void lane_down(Coordinator &c, std::string_view node_name, std::string_view fqn)
{
    c.m_retry.erase_pair(node_name, fqn);
    erase_ring(c.m_held, node_name, fqn);
    if(!any_holder(c.m_held, fqn))
        c.m_hints.erase(std::string{fqn});
}

}

#endif
