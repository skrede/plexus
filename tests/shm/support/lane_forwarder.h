#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_LANE_FORWARDER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_LANE_FORWARDER_H

#include "support/order_forwarder.h"
#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include "plexus/log/logger.h"

#include "plexus/topic_qos.h"

#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::testing {

// A publisher's forwarder bound to its coordinator the way the engine binds them. The lane's ring is an order_channel
// whose backpressure the test sets and whose writes land in ring_log; a released lane leaves the forwarder first.
struct lane_forwarder
{
    explicit lane_forwarder(const topic_qos &qos = topic_qos{})
            : ring_occupancy(0)
            , fwd(sink)
            , lanes(registry)
    {
        lanes.on_gate([this](std::string_view)
                      { return io::upgrade_mint<order_channel>{std::make_unique<order_channel>(ring_log, ring_occupancy, 99), [](std::size_t) { return true; }}; });
        lanes.on_lane_released([this](order_channel &channel) { fwd.forget_channel(channel); });
        lanes.set_topic_hint("t", io::dispatch_hint::frequent);
        lanes.on_local_publisher("t", true);
        fwd.on_demand_transition([this](std::string_view node, std::string_view fqn, io::demand_transition dir, io::demand_role role) { lanes.on_edge(node, fqn, dir, role); });
        fwd.on_companion_route([this](std::string_view node, std::string_view fqn, std::size_t bytes) { return lanes.companion_for(node, fqn, bytes); });
        fwd.on_companion({.event_cb = {}, .switch_cb = {}, .place_cb = {}, .offer_cb = {}, .lane_cb = [this](std::string_view fqn) { return lanes.lane_for(fqn); }});
        fwd.declare("t", qos);
    }

    // A co-host subscriber whose demand reached the publisher; ready also answers the offered lane.
    void subscribe(order_channel &wire, const std::string &name, bool ready = true)
    {
        fwd.attach_for_fanout(order_forwarder::peer{wire, name}, "t");
        if(ready)
            lanes.on_companion(name, "t", io::companion_event::ready);
    }

    void attach_self()
    {
        fwd.attach_local("t", *lanes.hold_self("t"), "self");
    }

    void publish(std::uint32_t count)
    {
        for(std::uint32_t n = 1; n <= count; ++n)
            fwd.publish("t", as_bytes(std::to_string(n)));
    }

    std::size_t ring_occupancy;
    arrival_log ring_log;
    co_host_registry registry;
    log::null_logger sink;
    order_forwarder fwd;
    io::upgrade_coordinator<co_host_registry, order_channel> lanes;
};

}

#endif
