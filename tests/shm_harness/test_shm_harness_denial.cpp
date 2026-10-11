#include "topology_launch.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <iostream>

using namespace shm_harness;

TEST_CASE("harness a subscriber denied io_uring still receives every message", "[shm][harness][!shouldfail]")
{
    topology topo{"shm_harness.denial." + std::to_string(plexus::testing::process_id()), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, true));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round(0, 20, {1}, 10s);
    const bool over_shm   = topo.region_attaches();
    const node_counts sub = topo.counts(1);
    std::cout << "denial " << summary(sub, over_shm) << std::endl;

    CHECK(topo.leave(1));
    CHECK(topo.leave(0));

    REQUIRE(over_shm);
    REQUIRE(sub.received == 20);
    REQUIRE(sub.distinct == 20);
    REQUIRE(sub.duplicates == 0);
}
