#include "lane_probe.h"
#include "topology_block.h"
#include "topology_launch.h"

#include "plexus/io/upgrade_channel.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>

using namespace shm_harness;

TEST_CASE("harness a hinted publisher delivers to an unhinted subscriber over the wire", "[shm][harness]")
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
    REQUIRE(sub.out_of_order == 0);
}

TEST_CASE("harness a subscriber past a ring's consumer capacity receives every message over the wire", "[shm][harness]")
{
    topology topo{probe_fqn("capacity"), 3};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false, node_composition::single_reader));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.spawn_node(2, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1, 2}, 10s));
    topo.publish_round({0}, 20, {1, 2}, 10s);
    const std::vector<node_counts> subs = report(topo, "capacity", {1, 2});
    leave_all(topo, {2, 1, 0});

    require_exactly(subs[0], 20);
    require_exactly(subs[1], 20);
    REQUIRE(subs[0].out_of_order == 0);
    REQUIRE(subs[1].out_of_order == 0);
    REQUIRE(fallbacks_of(subs[0], plexus::io::fallback_reason::consumer_capacity) + fallbacks_of(subs[1], plexus::io::fallback_reason::consumer_capacity) >= 1);
}

TEST_CASE("harness an unhinted node subscribing to its own topic serves a co-host subscriber once over the wire", "[shm][harness]")
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
    REQUIRE(fallbacks_of(subs[1], plexus::io::fallback_reason::not_offered) >= 1);
}
