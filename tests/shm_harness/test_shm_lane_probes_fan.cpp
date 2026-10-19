#include "lane_probe.h"
#include "launch_helpers.h"
#include "topology_block.h"
#include "topology_launch.h"

#include <catch2/catch_test_macros.hpp>

using namespace shm_harness;

// The duplicate comes from the two sessions a crossed dial leaves standing, not from the ring.
TEST_CASE("lane_probe an unhinted pair that dials each other delivers each message exactly once", "[shm][lane_probe][!shouldfail]")
{
    topology topo{probe_fqn("crossed"), 2, dial_wiring::crossed};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, false, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, false, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const node_counts sub = report(topo, "crossed", {1})[0];
    leave_all(topo, {1, 0});

    require_exactly(sub, 20);
}
