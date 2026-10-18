#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_LANES_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_LANES_H

#include "plexus/io/detail/upgrade_retry.h"
#include "plexus/io/detail/upgrade_rings.h"

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"

#include <chrono>
#include <string>
#include <utility>
#include <string_view>

namespace plexus::io::detail {

// The coordinator's lane bodies, befriended as one. A publisher mints its send lane on a subscriber's demand, before its
// response offers it; a subscriber attaches its receive lane only on that offer.
struct upgrade_lanes
{
    template<typename Coordinator>
    static bool awaits_offer(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        const auto *lane = find_ring(c.m_held, node_name, fqn, demand_role::subscriber);
        return lane != nullptr && !lane->receive.engaged();
    }

    template<typename Coordinator>
    static void send(Coordinator &c, std::string_view node_name, std::string_view fqn, companion_event event)
    {
        if(c.m_companion_send_cb)
            c.m_companion_send_cb(node_name, fqn, event);
    }

    // A declined gate keeps the wire receive path: nothing is retained and nothing is answered.
    template<typename Coordinator>
    static fallback_reason attach(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        if(!c.m_receive_mint || !awaits_offer(c, node_name, fqn))
            return fallback_reason::none;
        upgrade_receive received = c.m_receive_mint(node_name, fqn);
        if(!received.engaged())
            return received.reason;
        auto *lane = find_ring(c.m_held, node_name, fqn, demand_role::subscriber);
        if(lane == nullptr)
            return fallback_reason::none;
        lane->receive = std::move(received);
        send(c, node_name, fqn, companion_event::ready);
        return fallback_reason::none;
    }

    // A declined gate keeps the wire send path: nothing is retained.
    template<typename Coordinator>
    static fallback_reason mint(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        if(!c.m_mint || find_ring(c.m_held, node_name, fqn, demand_role::publisher) != nullptr)
            return fallback_reason::none;
        auto minted = c.m_mint(fqn);
        if(!minted.channel)
            return minted.reason;
        c.m_held[std::string{node_name}].push_back(
                {.fqn = std::string{fqn}, .role = demand_role::publisher, .ready = false, .channel = std::move(minted.channel), .fits = std::move(minted.fits), .receive = {}});
        return fallback_reason::none;
    }

    // The reason a gate declined the lane; none when the lane engaged, is already held or the policy keeps the wire.
    template<typename Coordinator>
    static fallback_reason engage(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
    {
        if(!c.run_policy(c.m_registry.same_host_for(node_name), c.hint_for(fqn)))
            return fallback_reason::none;
        return role == demand_role::subscriber ? attach(c, node_name, fqn) : mint(c, node_name, fqn);
    }

    template<typename Coordinator>
    static void arm(Coordinator &c)
    {
        if(const std::chrono::milliseconds delay = c.m_retry.arm(); delay.count() != 0)
            c.m_schedule_retry_cb(delay);
    }

    // A transient decline leaves the pair on the wire and posts a re-attempt; it never waits for the creator here.
    template<typename Coordinator>
    static void attempt(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
    {
        const fallback_reason reason = engage(c, node_name, fqn, role);
        c.count_fallback(reason);
        if(!retryable(reason) || !c.m_schedule_retry_cb)
            return;
        c.m_retry.defer(node_name, fqn, role);
        arm(c);
    }

    template<typename Coordinator>
    static void up(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
    {
        if(role == demand_role::publisher)
            return attempt(c, node_name, fqn, role);
        if(find_ring(c.m_held, node_name, fqn, role) == nullptr)
            c.m_held[std::string{node_name}].push_back({.fqn = std::string{fqn}, .role = role, .ready = false, .channel = nullptr, .fits = {}, .receive = {}});
    }

    // Each attempt counts its own decline; a pair that runs out of attempts also counts retries_exhausted, once.
    template<typename Coordinator>
    static void reattempt(Coordinator &c, const retry_key &key)
    {
        const fallback_reason reason = engage(c, key.node_name, key.fqn, key.role);
        c.count_fallback(reason);
        if(const fallback_reason stands = c.m_retry.settle(key, reason); stands != reason)
            c.count_fallback(stands);
    }

    // A gate may tear a pair down while this wake runs, so each due pair is re-checked before its re-attempt.
    template<typename Coordinator>
    static void retry(Coordinator &c)
    {
        for(const retry_key &key : c.m_retry.take_due())
            if(c.m_retry.contains(key))
                reattempt(c, key);
        arm(c);
    }

    // The last demand for the fqn gone drops its hint, so a churn of distinct topic names cannot grow the hint table
    // without bound (a re-subscribe re-records it from the declare path).
    template<typename Coordinator>
    static void down(Coordinator &c, std::string_view node_name, std::string_view fqn, demand_role role)
    {
        c.m_retry.erase_pair(node_name, fqn, role);
        erase_ring(c.m_held, node_name, fqn, role);
        if(!any_holder(c.m_held, fqn))
            c.m_hints.erase(std::string{fqn});
    }

    // A withheld offer is counted only when the subscriber's own policy wanted the ring.
    template<typename Coordinator>
    static void offer(Coordinator &c, std::string_view node_name, std::string_view fqn, bool offered)
    {
        if(!awaits_offer(c, node_name, fqn))
            return;
        if(offered)
            return attempt(c, node_name, fqn, demand_role::subscriber);
        if(c.run_policy(c.m_registry.same_host_for(node_name), c.hint_for(fqn)))
            c.count_fallback(fallback_reason::not_offered);
    }

    template<typename Coordinator>
    static void answer(Coordinator &c, std::string_view node_name, std::string_view fqn, companion_event event)
    {
        auto *lane = find_ring(c.m_held, node_name, fqn, demand_role::publisher);
        if(lane == nullptr)
            return;
        if(event == companion_event::ready)
        {
            lane->ready = true;
            return;
        }
        erase_ring(c.m_held, node_name, fqn, demand_role::publisher);
        c.count_fallback(fallback_reason::withdrawn);
    }

    template<typename Coordinator, typename Lane>
    static void withdraw(Coordinator &c, std::string_view node_name, Lane &lane)
    {
        lane.receive = upgrade_receive{};
        c.count_fallback(fallback_reason::withdrawn);
        send(c, node_name, lane.fqn, companion_event::withdrawn);
    }

    // Frames the publisher writes between the subscriber's last drain and its receipt of the withdrawal are not
    // delivered; the withdrawal counts that gap on each end. The subscriber keeps its demand for a later offer.
    template<typename Coordinator>
    static void lost(Coordinator &c, std::string_view fqn)
    {
        c.m_retry.erase_fqn(fqn);
        erase_fqn(c.m_held, fqn, demand_role::publisher);
        for(auto &[node_name, lanes] : c.m_held)
            for(auto &lane : lanes)
                if(lane.fqn == fqn && lane.receive.engaged())
                    withdraw(c, node_name, lane);
    }
};

}

#endif
