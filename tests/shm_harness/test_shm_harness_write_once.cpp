#include "lane_probe.h"
#include "topology_block.h"
#include "topology_launch.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>

using namespace shm_harness;

TEST_CASE("harness one publisher with two hinted subscribers delivers each message exactly once", "[shm][harness]")
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
    REQUIRE(subs[0].out_of_order == 0);
    REQUIRE(subs[1].out_of_order == 0);
}

TEST_CASE("harness a hinted node subscribing to its own topic delivers each message once to itself and to a co-host subscriber", "[shm][harness]")
{
    topology topo{probe_fqn("self_remote_hinted"), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pubsub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {0, 1}, 10s));
    topo.publish_round({0}, 20, {0, 1}, 10s);
    const std::vector<node_counts> subs = report(topo, "self_remote_hinted", {0, 1});
    leave_all(topo, {1, 0});

    require_exactly(subs[0], 20);
    require_exactly(subs[1], 20);
    REQUIRE(subs[0].out_of_order == 0);
    REQUIRE(subs[1].out_of_order == 0);
}

TEST_CASE("harness a hinted pair that dials each other delivers each message exactly once over shared memory", "[shm][harness]")
{
    topology topo{probe_fqn("crossed_hinted"), 2, dial_wiring::crossed};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const node_counts sub = report(topo, "crossed_hinted", {1})[0];
    const bool over_shm   = topo.regions().attaches();
    leave_all(topo, {1, 0});

    require_exactly(sub, 20);
    REQUIRE(sub.out_of_order == 0);
    REQUIRE(over_shm);
}
