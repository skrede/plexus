#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_COMPANION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_COMPANION_H

#include "plexus/io/companion_event.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"
#include "plexus/wire/topic_hash.h"

#include <optional>
#include <string_view>

namespace plexus::io::detail {

// event_cb hears what a co-host peer said about the shared-memory lane of one of the node's topics; holds_lane_cb answers
// whether the node offers a subscriber its stamped lane, which the subscribe response then carries; lane_cb names the
// channel of a topic's lane, which every subscriber routed to it shares.
template<typename Channel>
struct companion_hooks
{
    plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event)> event_cb;
    plexus::detail::move_only_function<bool(std::string_view, std::string_view)> holds_lane_cb;
    plexus::detail::move_only_function<Channel *(std::string_view)> lane_cb;
};

template<typename Endpoint, typename Channel>
void send_companion(Endpoint &endpoint, Channel &channel, std::string_view fqn, companion_event event)
{
    if(const std::optional<wire::msg_type> verb = verb_of(event))
        endpoint.send_control(channel, *verb, wire::encode_companion({wire::fqn_topic_hash(fqn)}));
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
