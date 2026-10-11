#include "topology_launch.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <iostream>

using namespace shm_harness;

TEST_CASE("harness one publisher and one hinted subscriber deliver every message once over shared memory", "[shm][harness]")
{
    topology topo{"shm_harness.baseline." + std::to_string(plexus::testing::process_id()), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const bool over_shm   = topo.regions().attaches();
    const node_counts sub = topo.counts(1);
    std::cout << "baseline " << summary(sub, over_shm) << std::endl;

    CHECK(topo.leave(1));
    CHECK(topo.leave(0));

    REQUIRE(over_shm);
    REQUIRE(sub.received == 20);
    REQUIRE(sub.distinct == 20);
    REQUIRE(sub.duplicates == 0);
}
