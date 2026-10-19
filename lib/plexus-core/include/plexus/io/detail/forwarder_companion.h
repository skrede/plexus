#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_COMPANION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_COMPANION_H

#include "plexus/io/companion_event.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"
#include "plexus/wire/topic_hash.h"

#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::detail {

// event_cb hears what a co-host peer said about the shared-memory lane of one of the node's topics, with the reader slot a
// ready names; switch_cb hears the sequence a co-host publisher switched this node to its lane at; place_cb places a
// subscriber's reader on the lane as the node switches it; holds_lane_cb answers whether the node offers a subscriber its
// stamped lane, which the subscribe response then carries; lane_cb names the channel of a topic's lane, which every
// subscriber routed to it shares.
template<typename Channel>
struct companion_hooks
{
    plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event, std::uint32_t)> event_cb;
    plexus::detail::move_only_function<void(std::string_view, std::string_view, std::uint64_t)> switch_cb;
    plexus::detail::move_only_function<bool(std::string_view, std::uint32_t)> place_cb;
    plexus::detail::move_only_function<bool(std::string_view, std::string_view)> holds_lane_cb;
    plexus::detail::move_only_function<Channel *(std::string_view)> lane_cb;
};

template<typename Endpoint, typename Channel>
void send_companion(Endpoint &endpoint, Channel &channel, std::string_view fqn, companion_event event, std::uint32_t reader)
{
    if(event == companion_event::ready)
        endpoint.send_control(channel, wire::msg_type::companion_ready, wire::encode_companion_ready({wire::fqn_topic_hash(fqn), reader}));
    else if(const std::optional<wire::msg_type> verb = verb_of(event))
        endpoint.send_control(channel, *verb, wire::encode_companion({wire::fqn_topic_hash(fqn)}));
}

// A forwarder with no placement hook switches without placing.
template<typename Channel>
bool place_reader(companion_hooks<Channel> &hooks, std::string_view fqn, std::uint32_t reader)
{
    return !hooks.place_cb || hooks.place_cb(fqn, reader);
}

template<typename Endpoint, typename Channel>
void send_switch(Endpoint &endpoint, Channel &channel, std::string_view fqn, std::uint64_t first_sequence)
{
    endpoint.send_control(channel, wire::msg_type::companion_switch, wire::encode_companion_switch({wire::fqn_topic_hash(fqn), first_sequence}));
}

template<typename Channel>
bool holds_lane(companion_hooks<Channel> &hooks, std::string_view node_name, std::string_view fqn)
{
    return hooks.holds_lane_cb && hooks.holds_lane_cb(node_name, fqn);
}

template<typename Channel>
Channel *lane_channel(companion_hooks<Channel> &hooks, std::string_view fqn)
{
    return hooks.lane_cb ? hooks.lane_cb(fqn) : nullptr;
}

}

#endif
