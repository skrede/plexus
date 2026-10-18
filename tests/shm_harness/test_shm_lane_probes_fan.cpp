#include "lane_probe.h"
#include "launch_helpers.h"
#include "topology_block.h"
#include "topology_launch.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>

using namespace shm_harness;

TEST_CASE("lane_probe one publisher with two hinted subscribers delivers each message exactly once", "[shm][lane_probe][!shouldfail]")
{
    topology topo{probe_fqn("fan_out"), 3};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.spawn_node(2, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1, 2}, 10s));
    topo.publish_round({0}, 20, {1, 2}, 10s);
    const std::vector<node_counts> subs = report(topo, "fan_out", {1, 2});
    leave_all(topo, {2, 1, 0});

    require_exactly(subs[0], 20);
    require_exactly(subs[1], 20);
}

TEST_CASE("lane_probe a node subscribing to its own topic does not duplicate delivery to a co-host subscriber", "[shm][lane_probe][!shouldfail]")
{
    topology topo{probe_fqn("self_remote"), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pubsub, false, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {0, 1}, 10s));
    topo.publish_round({0}, 20, {0, 1}, 10s);
    const std::vector<node_counts> subs = report(topo, "self_remote", {0, 1});
    leave_all(topo, {1, 0});

    require_exactly(subs[0], 20);
    require_exactly(subs[1], 20);
}

TEST_CASE("lane_probe a hinted publisher delivers to an unhinted subscriber", "[shm][lane_probe][!shouldfail]")
{
    topology topo{probe_fqn("hint"), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, false, false));
    REQUIRE(topo.wait_connected(10s));

    const bool warmed = topo.warm_up({0}, {1}, 10s);
    topo.publish_round({0}, 20, {1}, 10s);
    const node_counts sub = report(topo, warmed ? "hint warmed=1" : "hint warmed=0", {1})[0];
    leave_all(topo, {1, 0});

    REQUIRE(warmed);
    require_exactly(sub, 20);
}

TEST_CASE("lane_probe a hinted pair that dials each other delivers each message exactly once", "[shm][lane_probe][!shouldfail]")
{
    topology topo{probe_fqn("crossed"), 2, dial_wiring::crossed};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const node_counts sub = report(topo, "crossed", {1})[0];
    leave_all(topo, {1, 0});

    require_exactly(sub, 20);
}
