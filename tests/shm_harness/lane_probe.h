#ifndef HPP_GUARD_TESTS_SHM_HARNESS_LANE_PROBE_H
#define HPP_GUARD_TESTS_SHM_HARNESS_LANE_PROBE_H

#include "topology_block.h"
#include "topology_launch.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <iostream>
#include <string_view>

namespace shm_harness {

inline std::string probe_fqn(std::string_view name)
{
    return "shm_harness." + std::string(name) + "." + std::to_string(plexus::testing::process_id());
}

inline std::vector<node_counts> report(topology &topo, std::string_view label, const std::vector<std::uint32_t> &nodes)
{
    const bool over_shm = topo.regions().attaches();
    std::vector<node_counts> counts;
    for(const std::uint32_t index : nodes)
    {
        counts.push_back(topo.counts(index));
        std::cout << label << " node" << index << " " << summary(counts.back(), over_shm) << std::endl;
    }
    return counts;
}

inline void leave_all(topology &topo, const std::vector<std::uint32_t> &nodes)
{
    for(const std::uint32_t index : nodes)
        CHECK(topo.leave(index));
}

inline void require_exactly(const node_counts &counts, std::uint32_t expected)
{
    REQUIRE(counts.received == expected);
    REQUIRE(counts.distinct == expected);
    REQUIRE(counts.duplicates == 0);
}

struct crash_restart_outcome
{
    node_counts subscriber;
    node_counts publisher;
    std::uint32_t regions_before;
    std::uint32_t regions_after;
};

// The subscriber is killed after a first round and respawned, and the publisher's send histogram is cleared before the
// round of count messages it then sends.
inline crash_restart_outcome crash_restart(std::string_view name, std::uint32_t count)
{
    topology topo{probe_fqn(name), 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_registry(0, node_role::pub));
    REQUIRE(topo.spawn_registry(1, node_role::sub));
    const bool first_round = topo.publish_round({0}, 10, {1}, 10s);
    report(topo, std::string(name) + " before", {1});
    REQUIRE(first_round);

    const std::uint32_t regions_before = topo.regions().entries();
    topo.kill(1);
    REQUIRE(topo.spawn_registry(1, node_role::sub));
    REQUIRE(topo.command(0, node_command::reset));
    topo.publish_round({0}, count, {1}, 60s);
    const node_counts subscriber = report(topo, std::string(name) + " after", {1})[0];
    const node_counts publisher  = topo.counts(0);
    leave_all(topo, {1, 0});
    const crash_restart_outcome outcome{subscriber, publisher, regions_before, topo.regions().entries()};
    std::cout << name << " p99_send_us=" << publisher.p99_send_us << " slowest_send_us=" << publisher.slowest_send_us << " regions_before=" << regions_before
              << " regions_after=" << outcome.regions_after << std::endl;
    return outcome;
}

}

#endif
