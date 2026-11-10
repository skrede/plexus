#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_SWITCH_RIG_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_SWITCH_RIG_H

#include "support/order_forwarder.h"

#include "plexus/io/upgrade_channel.h"

#include "plexus/log/logger.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

namespace plexus::testing {

// Whether publish n has a lane for the subscriber, and whether it fits that lane.
struct scripted_route
{
    bool lane;
    bool fits;
};

// One subscriber of topic "t" on a wire and a ring channel, the route to the ring scripted per publish.
struct switch_rig
{
    explicit switch_rig(std::function<scripted_route(std::uint32_t)> route)
            : log()
            , wire_occupancy(0)
            , ring_occupancy(0)
            , wire(log, wire_occupancy, 1)
            , ring(log, ring_occupancy, 2)
            , sink()
            , fwd(sink)
            , n(0)
            , script(std::move(route))
    {
        fwd.declare("t", plexus::topic_qos{});
        REQUIRE(fwd.attach_for_fanout(order_forwarder::peer{wire, "node-a"}, "t"));
        fwd.on_companion_route(
                [this](std::string_view, std::string_view, std::size_t)
                {
                    const scripted_route step = script(n);
                    return plexus::io::lane_route<order_channel>{step.lane ? &ring : nullptr, step.fits, plexus::io::k_no_reader};
                });
    }

    void publish(std::uint32_t count)
    {
        for(n = 1; n <= count; ++n)
            fwd.publish("t", as_bytes(std::to_string(n)));
    }

    std::uint64_t sequence_of(const std::string &body) const
    {
        for(const carried_frame &frame : ring.frames())
            if(frame.body == body)
                return frame.sequence;
        return 0;
    }

    arrival_log log;
    std::size_t wire_occupancy;
    std::size_t ring_occupancy;
    order_channel wire;
    order_channel ring;
    plexus::log::null_logger sink;
    order_forwarder fwd;
    std::uint32_t n;
    std::function<scripted_route(std::uint32_t)> script;
};

inline std::vector<std::string> bodies(const order_channel &channel)
{
    std::vector<std::string> out;
    for(const carried_frame &frame : channel.frames())
        out.push_back(frame.body);
    return out;
}

}

#endif
