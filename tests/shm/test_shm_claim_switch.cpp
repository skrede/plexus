#include "support/order_forwarder.h"
#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include "plexus/log/logger.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <deque>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

using plexus::io::reader_claim;
using plexus::io::demand_role;
using plexus::io::companion_event;
using plexus::io::demand_transition;
using plexus::testing::as_bytes;
using plexus::testing::arrival_log;
using plexus::testing::order_channel;
using plexus::testing::order_forwarder;
using plexus::testing::co_host_registry;

namespace {

// A publisher whose lane places a reader as the ring's cursor swap does: a claim lands once.
struct placing_publisher
{
    placing_publisher()
            : ring_occupancy(0)
            , wire_occupancy(0)
            , ring_log()
            , wire_log()
            , registry()
            , sink()
            , fwd(sink)
            , wires()
            , placed()
            , lanes(registry)
    {
        lanes.on_gate([this](std::string_view)
                      { return plexus::io::upgrade_mint<order_channel>{std::make_unique<order_channel>(ring_log, ring_occupancy, 99), [](std::size_t) { return true; }, [this](reader_claim claim) { return land(claim); }}; });
        lanes.on_lane_released([this](order_channel &channel) { fwd.forget_channel(channel); });
        lanes.set_topic_hint("t", plexus::io::dispatch_hint::frequent);
        lanes.on_local_publisher("t", true);
        fwd.on_demand_transition([this](std::string_view node, std::string_view fqn, demand_transition dir, demand_role role) { lanes.on_edge(node, fqn, dir, role); });
        fwd.on_companion_route([this](std::string_view node, std::string_view fqn, std::size_t bytes) { return lanes.companion_for(node, fqn, bytes); });
        fwd.on_companion({.event_cb  = {},
                          .switch_cb = {},
                          .place_cb  = [this](std::string_view fqn, reader_claim claim) { return lanes.place(fqn, claim); },
                          .offer_cb  = {},
                          .lane_cb   = [this](std::string_view fqn) { return lanes.lane_for(fqn); }});
        fwd.declare("t", plexus::topic_qos{});
    }

    bool land(reader_claim claim)
    {
        if(std::ranges::find(placed, claim) != placed.end())
            return false;
        placed.push_back(claim);
        return true;
    }

    // A co-host session whose demand reached the publisher and whose ready names claim.
    void subscribe(const std::string &name, reader_claim claim)
    {
        order_channel &wire = wires.emplace_back(wire_log, wire_occupancy, wires.size() + 1);
        fwd.attach_for_fanout(order_forwarder::peer{wire, name}, "t");
        lanes.on_companion(name, "t", companion_event::ready, claim);
    }

    void publish(std::uint32_t from, std::uint32_t to)
    {
        for(std::uint32_t n = from; n <= to; ++n)
            fwd.publish("t", as_bytes(std::to_string(n)));
    }

    std::size_t ring_occupancy;
    std::size_t wire_occupancy;
    arrival_log ring_log;
    arrival_log wire_log;
    co_host_registry registry;
    plexus::log::null_logger sink;
    order_forwarder fwd;
    std::deque<order_channel> wires;
    std::vector<reader_claim> placed;
    plexus::io::upgrade_coordinator<co_host_registry, order_channel> lanes;
};

arrival_log numbered(std::uint32_t from, std::uint32_t to)
{
    arrival_log out;
    for(std::uint32_t n = from; n <= to; ++n)
        out.push_back(std::to_string(n));
    return out;
}

}

TEST_CASE("claim_switch a second session holding a claim another session placed switches without placing it again", "[shm][claim_switch][!shouldfail]")
{
    placing_publisher p;
    p.subscribe("peer-a", reader_claim{3, 7});
    p.subscribe("peer-b", reader_claim{3, 7});

    p.publish(1, 5);

    CHECK(p.placed.size() == 1);
    CHECK(p.ring_log == numbered(1, 5));
    CHECK(p.wire_log.empty());
}

TEST_CASE("claim_switch a session that readies a claim already placed switches on its first publish", "[shm][claim_switch][!shouldfail]")
{
    placing_publisher p;
    p.subscribe("peer-a", reader_claim{3, 7});
    p.publish(1, 2);
    p.subscribe("peer-b", reader_claim{3, 7});

    p.publish(3, 5);

    CHECK(p.placed.size() == 1);
    CHECK(p.ring_log == numbered(1, 5));
    CHECK(p.wire_log.empty());
}

TEST_CASE("claim_switch sessions holding distinct claims each place their own", "[shm][claim_switch]")
{
    placing_publisher p;
    p.subscribe("peer-a", reader_claim{3, 7});
    p.subscribe("peer-b", reader_claim{4, 1});

    p.publish(1, 5);

    CHECK(p.placed == std::vector<reader_claim>{reader_claim{3, 7}, reader_claim{4, 1}});
    CHECK(p.ring_log == numbered(1, 5));
    CHECK(p.wire_log.empty());
}
