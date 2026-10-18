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
// whether the node holds a stamped ring for a subscriber, which the subscribe response then offers.
struct companion_hooks
{
    plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event)> event_cb;
    plexus::detail::move_only_function<bool(std::string_view, std::string_view)> holds_lane_cb;
};

template<typename Endpoint, typename Channel>
void send_companion(Endpoint &endpoint, Channel &channel, std::string_view fqn, companion_event event)
{
    if(const std::optional<wire::msg_type> verb = verb_of(event))
        endpoint.send_control(channel, *verb, wire::encode_companion({wire::fqn_topic_hash(fqn)}));
}

template<typename Hooks>
bool holds_lane(Hooks &hooks, std::string_view node_name, std::string_view fqn)
{
    return hooks.holds_lane_cb && hooks.holds_lane_cb(node_name, fqn);
}

}

#endif
