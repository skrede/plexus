#include "topology_launch.h"

#include "plexus/io/upgrade_channel.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <iostream>

using namespace shm_harness;

TEST_CASE("harness both ends denied every notifier deliver every message over the wire", "[shm][harness]")
{
    topology topo{"shm_harness.wire." + std::to_string(plexus::testing::process_id()), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false, node_composition::arm_denied));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false, node_composition::arm_denied));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const bool over_shm   = topo.regions().attaches();
    const node_counts pub = topo.counts(0);
    const node_counts sub = topo.counts(1);
    std::cout << "wire " << summary(sub, over_shm) << " publisher fallbacks=" << fallback_summary(pub) << std::endl;

    CHECK(topo.leave(1));
    CHECK(topo.leave(0));

    REQUIRE_FALSE(over_shm);
    REQUIRE(sub.received == 20);
    REQUIRE(sub.distinct == 20);
    REQUIRE(sub.duplicates == 0);
    REQUIRE(fallbacks_of(pub, plexus::io::fallback_reason::notifier_denied) >= 1);
    REQUIRE(fallbacks_of(sub, plexus::io::fallback_reason::not_offered) >= 1);
}

TEST_CASE("harness both ends that lose every notifier at runtime deliver every message over the wire", "[shm][harness]")
{
    topology topo{"shm_harness.lost." + std::to_string(plexus::testing::process_id()), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false, node_composition::runtime_lost));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false, node_composition::runtime_lost));
    REQUIRE(topo.wait_connected(10s));

    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const bool over_shm   = topo.regions().attaches();
    const node_counts pub = topo.counts(0);
    const node_counts sub = topo.counts(1);
    std::cout << "lost " << summary(sub, over_shm) << " publisher fallbacks=" << fallback_summary(pub) << std::endl;

    CHECK(topo.leave(1));
    CHECK(topo.leave(0));

    REQUIRE_FALSE(over_shm);
    REQUIRE(sub.received == 20);
    REQUIRE(sub.distinct == 20);
    REQUIRE(sub.duplicates == 0);
}
