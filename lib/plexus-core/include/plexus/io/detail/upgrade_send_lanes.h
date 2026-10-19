#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_SEND_LANES_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_SEND_LANES_H

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <string_view>

// Inside plexus::io::detail, plexus::detail must be fully qualified — io::detail shadows it.
namespace plexus::io::detail {

// A co-host peer the publisher offered its lane to; the lane carries its messages only once it answered ready.
struct lane_holder
{
    std::string node_name;
    std::uint32_t reader;
    bool ready;
};

// A publisher's one lane for a topic: one channel over its own ring, which every holder reads, so one publish writes it
// once. It lives while an offered peer or the node's own subscriber holds it. fits(bytes) is the minting medium's route
// predicate: true when a message rides the lane, false when it falls back to the wire.
template<typename Channel>
struct send_lane
{
    std::string fqn;
    std::uint32_t self_holds;
    std::vector<lane_holder> holders;
    std::unique_ptr<Channel> channel;
    plexus::detail::move_only_function<bool(std::size_t)> fits;
    plexus::detail::move_only_function<bool(std::uint32_t)> place;
};

template<typename Channel>
using send_lanes = std::vector<send_lane<Channel>>;

template<typename Channel>
send_lane<Channel> *find_send_lane(send_lanes<Channel> &lanes, std::string_view fqn)
{
    const auto it = std::ranges::find(lanes, fqn, &send_lane<Channel>::fqn);
    return it == lanes.end() ? nullptr : &*it;
}

template<typename Channel>
const send_lane<Channel> *find_send_lane(const send_lanes<Channel> &lanes, std::string_view fqn)
{
    return find_send_lane(const_cast<send_lanes<Channel> &>(lanes), fqn);
}

inline lane_holder *find_holder(std::vector<lane_holder> &holders, std::string_view node_name)
{
    const auto it = std::ranges::find(holders, node_name, &lane_holder::node_name);
    return it == holders.end() ? nullptr : &*it;
}

template<typename Channel>
bool offered(const send_lanes<Channel> &lanes, std::string_view node_name, std::string_view fqn)
{
    const send_lane<Channel> *lane = find_send_lane(lanes, fqn);
    return lane != nullptr && std::ranges::find(lane->holders, node_name, &lane_holder::node_name) != lane->holders.end();
}

// The lane's channel, or with self_only set, its channel only while the node's own subscriber holds it.
template<typename Channel>
Channel *send_lane_channel(const send_lanes<Channel> &lanes, std::string_view fqn, bool self_only)
{
    const send_lane<Channel> *lane = find_send_lane(lanes, fqn);
    return lane == nullptr || (self_only && lane->self_holds == 0) ? nullptr : lane->channel.get();
}

// The lane's channel for a holder that answered ready, and whether this message fits it; with no lane the pair keeps the
// wire. Taken mutably: fits is a move_only_function with a non-const call operator.
template<typename Channel>
lane_route<Channel> route_send_lane(send_lanes<Channel> &lanes, std::string_view node_name, std::string_view fqn, std::size_t bytes)
{
    send_lane<Channel> *lane  = find_send_lane(lanes, fqn);
    const lane_holder *holder = lane == nullptr ? nullptr : find_holder(lane->holders, node_name);
    if(holder == nullptr || !holder->ready)
        return {.lane = nullptr, .fits = false, .reader = k_no_reader};
    return {.lane = lane->channel.get(), .fits = lane->fits && lane->fits(bytes), .reader = holder->reader};
}

// The coordinator's send-lane bodies, befriended as one. Every path that destroys a lane goes through release.
struct upgrade_send_lanes
{
    // A declined gate retains nothing and returns its reason. A minted lane serves every peer still waiting on a
    // re-attempt for it.
    template<typename Coordinator>
    static fallback_reason ensure(Coordinator &c, std::string_view fqn)
    {
        if(!c.m_mint || find_send_lane(c.m_send, fqn) != nullptr)
            return fallback_reason::none;
        auto minted = c.m_mint(fqn);
        if(!minted.channel)
            return minted.reason;
        c.m_send.push_back(
                {.fqn = std::string{fqn}, .self_holds = 0, .holders = {}, .channel = std::move(minted.channel), .fits = std::move(minted.fits), .place = std::move(minted.place)});
        for(const std::string &node_name : c.m_retry.take_waiting(fqn, demand_role::publisher))
            join(c, node_name, fqn);
        return fallback_reason::none;
    }

    // The peer becomes an offered holder before its subscribe response can offer the lane.
    template<typename Coordinator>
    static fallback_reason join(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        const fallback_reason reason = ensure(c, fqn);
        auto *lane                   = find_send_lane(c.m_send, fqn);
        if(lane == nullptr)
            return reason;
        if(find_holder(lane->holders, node_name) == nullptr)
            lane->holders.push_back({.node_name = std::string{node_name}, .reader = k_no_reader, .ready = false});
        return fallback_reason::none;
    }

    // The hook forgets the channel everywhere it is recorded while the channel still lives; only then is it destroyed.
    template<typename Coordinator>
    static void release(Coordinator &c, std::string_view fqn)
    {
        const auto it = std::ranges::find(c.m_send, fqn, &send_lane<typename Coordinator::channel_type>::fqn);
        if(it == c.m_send.end())
            return;
        const std::unique_ptr<typename Coordinator::channel_type> channel = std::move(it->channel);
        c.m_send.erase(it);
        if(c.m_lane_released_cb)
            c.m_lane_released_cb(*channel);
    }

    template<typename Coordinator>
    static void release_if_unheld(Coordinator &c, std::string_view fqn)
    {
        const auto *lane = find_send_lane(c.m_send, fqn);
        if(lane != nullptr && lane->holders.empty() && lane->self_holds == 0)
            release(c, fqn);
    }

    template<typename Coordinator>
    static void leave(Coordinator &c, std::string_view node_name, std::string_view fqn)
    {
        if(auto *lane = find_send_lane(c.m_send, fqn))
            std::erase_if(lane->holders, [&](const lane_holder &h) { return h.node_name == node_name; });
        release_if_unheld(c, fqn);
    }

    // A dead peer's receive lanes and pending re-attempts go with it.
    template<typename Coordinator>
    static void forget_peer(Coordinator &c, std::string_view node_name)
    {
        c.m_retry.erase_peer(node_name);
        c.m_receive.erase(std::string{node_name});
        std::vector<std::string> held;
        for(const auto &lane : c.m_send)
            if(std::ranges::find(lane.holders, node_name, &lane_holder::node_name) != lane.holders.end())
                held.push_back(lane.fqn);
        for(const std::string &fqn : held)
            leave(c, node_name, fqn);
    }

    // The node's own subscriber holds the lane whatever the cross-process policy says; a declined mint is counted.
    template<typename Coordinator>
    static typename Coordinator::channel_type *hold_self(Coordinator &c, std::string_view fqn)
    {
        c.count_fallback(ensure(c, fqn));
        auto *lane = find_send_lane(c.m_send, fqn);
        if(lane == nullptr)
            return nullptr;
        ++lane->self_holds;
        return lane->channel.get();
    }

    template<typename Coordinator>
    static void release_self(Coordinator &c, std::string_view fqn)
    {
        auto *lane = find_send_lane(c.m_send, fqn);
        if(lane == nullptr || lane->self_holds == 0)
            return;
        --lane->self_holds;
        release_if_unheld(c, fqn);
    }
};

}

#endif
