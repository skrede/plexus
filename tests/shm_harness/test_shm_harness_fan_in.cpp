#include "lane_probe.h"
#include "launch_helpers.h"
#include "topology_block.h"
#include "topology_launch.h"

#include <catch2/catch_test_macros.hpp>

using namespace shm_harness;

// Each publisher of a topic owns its own ring and its own entry and sink on the subscriber, so one publisher leaving
// tears down only its own lane.
TEST_CASE("harness a surviving publisher keeps reaching the subscriber after another publisher leaves", "[shm][harness]")
{
    topology topo{probe_fqn("fan_in"), 3};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(2, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0, 1}, {2}, 10s));
    topo.publish_round({0, 1}, 10, {2}, 10s);
    const node_counts both = report(topo, "fan_in both_publishers", {2})[0];
    CHECK(topo.leave(1));
    const bool departed = wait_until([&] { return topo.counts(2).connected == 1; }, 10s);
    topo.publish_round({0}, 10, {2}, 10s);
    const node_counts survivor = report(topo, "fan_in survivor", {2})[0];
    leave_all(topo, {2, 0});

    require_exactly(both, 20);
    REQUIRE(departed);
    require_exactly(survivor, 10);
}
