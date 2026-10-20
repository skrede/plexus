#include "lane_probe.h"
#include "topology_block.h"
#include "topology_launch.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <iostream>

using namespace shm_harness;

TEST_CASE("lane_probe a subscriber survives a clean restart of the ring's creator", "[shm][lane_probe]")
{
    topology topo{probe_fqn("creator_restart"), 3};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_registry(0, node_role::pub));
    REQUIRE(topo.spawn_registry(1, node_role::sub));
    REQUIRE(topo.spawn_registry(2, node_role::sub));

    const bool first_round = topo.publish_round({0}, 20, {1, 2}, 10s);
    report(topo, "creator_restart before", {1, 2});
    REQUIRE(first_round);

    REQUIRE(topo.leave(0));
    REQUIRE(topo.spawn_registry(0, node_role::pub));
    topo.publish_round({0}, 20, {1, 2}, 10s);
    const std::vector<node_counts> subs = report(topo, "creator_restart after", {1, 2});
    leave_all(topo, {2, 1, 0});

    require_exactly(subs[0], 20);
    require_exactly(subs[1], 20);
    REQUIRE(topo.regions().entries() == 0);
}

TEST_CASE("lane_probe a subscriber restarted after a crash receives every message", "[shm][lane_probe]")
{
    topology topo{probe_fqn("crash_restart"), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_registry(0, node_role::pub));
    REQUIRE(topo.spawn_registry(1, node_role::sub));

    const bool first_round = topo.publish_round({0}, 10, {1}, 10s);
    report(topo, "crash_restart before", {1});
    REQUIRE(first_round);

    const std::uint32_t regions_before = topo.regions().entries();
    topo.kill(1);
    REQUIRE(topo.spawn_registry(1, node_role::sub));
    REQUIRE(topo.command(0, node_command::reset));
    topo.publish_round({0}, 100, {1}, 60s);
    const node_counts sub       = report(topo, "crash_restart after", {1})[0];
    const node_counts publisher = topo.counts(0);
    leave_all(topo, {1, 0});
    const std::uint32_t regions_after = topo.regions().entries();
    std::cout << "crash_restart p99_send_us=" << publisher.p99_send_us << " slowest_send_us=" << publisher.slowest_send_us << " regions_before=" << regions_before
              << " regions_after=" << regions_after << std::endl;

    require_exactly(sub, 100);
    REQUIRE(publisher.p99_send_us <= 1000);
    REQUIRE(regions_after == 0);
}

TEST_CASE("lane_probe a node subscriber keeps receiving across a clean restart of the publishing node", "[shm][lane_probe]")
{
    topology topo{probe_fqn("node_restart"), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));
    CHECK(topo.warm_up({0}, {1}, 10s));
    const bool first_round = topo.publish_round({0}, 20, {1}, 10s);
    report(topo, "node_restart before", {1});
    REQUIRE(first_round);

    REQUIRE(topo.leave(0));
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    topo.forget_warm(1);
    const auto respawned = std::chrono::steady_clock::now();
    REQUIRE(topo.warm_up({0}, {1}, 30s));
    const auto rejoined = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - respawned);
    topo.publish_round({0}, 20, {1}, 10s);
    const node_counts sub = report(topo, "node_restart after", {1})[0];
    std::cout << "node_restart rejoined_ms=" << rejoined.count() << std::endl;
    const bool over_shm = topo.regions().attaches();
    leave_all(topo, {1, 0});

    require_exactly(sub, 20);
    REQUIRE(over_shm);
    REQUIRE(topo.regions().entries() == 0);
}
