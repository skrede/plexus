#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_LANE_PROBE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_LANE_PROBE_H

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

}

#endif
