#include "support/switch_rig.h"
#include "support/order_forwarder.h"
#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"
#include "plexus/io/detail/egress_scheduler.h"

#include "plexus/log/logger.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::testing::bodies;
using plexus::testing::as_bytes;
using plexus::testing::switch_rig;
using plexus::testing::arrival_log;
using plexus::testing::order_channel;
using plexus::testing::scripted_route;
using plexus::testing::order_forwarder;

TEST_CASE("switch_order a late companion route never overtakes frames queued on the wire", "[shm][switch_order]")
{
    constexpr std::uint32_t k_switch_at  = 4;
    constexpr std::uint32_t k_unstall_at = 8;
    constexpr std::uint32_t k_publishes  = 12;
    arrival_log log;
    std::size_t wire_occupancy = plexus::io::detail::k_low_water + 1;
    std::size_t ring_occupancy = 0;
    order_channel wire{log, wire_occupancy, 1};
    order_channel ring{log, ring_occupancy, 2};
    plexus::log::null_logger sink;
    order_forwarder fwd{sink};
    fwd.declare("t", plexus::topic_qos{});
    REQUIRE(fwd.attach_for_fanout(order_forwarder::peer{wire, "node-a"}, "t"));

    std::uint32_t n = 0;
    fwd.on_companion_route([&](std::string_view, std::string_view, std::size_t)
                           { return plexus::io::lane_route<order_channel>{n >= k_switch_at ? &ring : nullptr, true, plexus::io::k_no_reader}; });
    arrival_log expected;
    for(n = 1; n <= k_publishes; ++n)
    {
        wire_occupancy = n >= k_unstall_at ? 0 : wire_occupancy;
        fwd.publish("t", as_bytes(std::to_string(n)));
        expected.push_back(std::to_string(n));
    }
    REQUIRE(log == expected);
}

// The publisher mints its lane on the subscribe and routes over it only once the subscriber answers ready, while the wire
// still holds a backlog that must leave first.
TEST_CASE("switch_order a lane that turns ready behind a wire backlog delivers every frame once and in order", "[shm][switch_order]")
{
    constexpr std::uint32_t k_ready_at   = 4;
    constexpr std::uint32_t k_unstall_at = 8;
    constexpr std::uint32_t k_publishes  = 12;
    arrival_log log;
    std::size_t wire_occupancy = plexus::io::detail::k_low_water + 1;
    std::size_t ring_occupancy = 0;
    order_channel wire{log, wire_occupancy, 1};
    order_channel *ring = nullptr;
    plexus::testing::co_host_registry registry;
    plexus::io::upgrade_coordinator<plexus::testing::co_host_registry, order_channel> lanes{registry};
    lanes.on_gate(
            [&](std::string_view)
            {
                auto minted = std::make_unique<order_channel>(log, ring_occupancy, 2);
                ring        = minted.get();
                return plexus::io::upgrade_mint<order_channel>{std::move(minted), [](std::size_t) { return true; }};
            });
    lanes.set_topic_hint("t", plexus::io::dispatch_hint::frequent);
    plexus::log::null_logger sink;
    order_forwarder fwd{sink};
    fwd.on_demand_transition([&](std::string_view node, std::string_view fqn, plexus::io::demand_transition dir, plexus::io::demand_role role) { lanes.on_edge(node, fqn, dir, role); });
    fwd.on_companion_route([&](std::string_view node, std::string_view fqn, std::size_t bytes) { return lanes.companion_for(node, fqn, bytes); });
    fwd.declare("t", plexus::topic_qos{});
    REQUIRE(fwd.attach_for_fanout(order_forwarder::peer{wire, "node-a"}, "t"));
    REQUIRE(ring != nullptr);

    arrival_log expected;
    for(std::uint32_t n = 1; n <= k_publishes; ++n)
    {
        if(n == k_ready_at)
        {
            REQUIRE(ring->carried() == 0);
            lanes.on_companion("node-a", "t", plexus::io::companion_event::ready);
        }
        wire_occupancy = n >= k_unstall_at ? 0 : wire_occupancy;
        fwd.publish("t", as_bytes(std::to_string(n)));
        expected.push_back(std::to_string(n));
    }
    REQUIRE(log == expected);
    REQUIRE(ring->carried() > 0);
    REQUIRE(wire.carried() + ring->carried() == k_publishes);
}

TEST_CASE("attach_order the subscriber records its demand before its subscribe leaves", "[shm][attach_order]")
{
    arrival_log log;
    std::size_t occupancy = 0;
    order_channel wire{log, occupancy, 3};
    plexus::log::null_logger sink;
    order_forwarder fwd{sink};
    fwd.on_demand_transition(
            [&](std::string_view, std::string_view, plexus::io::demand_transition dir, plexus::io::demand_role role)
            {
                if(dir == plexus::io::demand_transition::up && role == plexus::io::demand_role::subscriber)
                    log.emplace_back("up");
            });

    REQUIRE(fwd.attach(order_forwarder::peer{wire, "node-a"}, "t"));
    REQUIRE(log == arrival_log{"up", "subscribe"});
}

TEST_CASE("switch_order the switch sends one marker naming the first lane-routed sequence", "[shm][switch_order]")
{
    switch_rig rig{[](std::uint32_t n) { return scripted_route{.lane = n >= 4, .fits = true}; }};
    rig.publish(12);

    REQUIRE(rig.wire.switches().size() == 1);
    CHECK(rig.ring.frames().front().body == "4");
    CHECK(rig.wire.switches().front() == rig.ring.frames().front().sequence);
}

// Moving the subscriber back for an over-cap message would put its next messages on the wire while other readers' copies
// of them land in the ring it reads.
TEST_CASE("switch_order an over-cap message after the switch keeps the subscriber on the lane", "[shm][switch_order]")
{
    switch_rig rig{[](std::uint32_t n) { return scripted_route{.lane = n >= 4, .fits = n != 6}; }};
    rig.publish(12);

    CHECK(bodies(rig.wire) == std::vector<std::string>{"1", "2", "3", "6"});
    CHECK(bodies(rig.ring) == std::vector<std::string>{"4", "5", "7", "8", "9", "10", "11", "12"});
    CHECK(rig.wire.switches().size() == 1);
}

TEST_CASE("switch_order a withdrawn lane returns the subscriber to the wire and a new engagement sends a new marker", "[shm][switch_order]")
{
    switch_rig rig{[](std::uint32_t n) { return scripted_route{.lane = n >= 4 && (n < 7 || n > 8), .fits = true}; }};
    rig.publish(12);

    CHECK(bodies(rig.wire) == std::vector<std::string>{"1", "2", "3", "7", "8"});
    CHECK(bodies(rig.ring) == std::vector<std::string>{"4", "5", "6", "9", "10", "11", "12"});
    CHECK(rig.wire.switches() == std::vector<std::uint64_t>{rig.sequence_of("4"), rig.sequence_of("9")});
}
