#include "lane_probe.h"
#include "node_traffic.h"
#include "topic_regions.h"
#include "launch_helpers.h"
#include "topology_block.h"
#include "topology_launch.h"

#include "plexus/asio/same_host_transports.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <string>
#include <cstdint>
#include <iostream>

using namespace shm_harness;

TEST_CASE("lane_probe a send-only companion with no consumer never refuses a burst", "[shm][lane_probe]")
{
    constexpr std::uint32_t burst = 600;
    const std::string fqn         = probe_fqn("burst");
    const topic_regions regions{fqn, {plexus::testing::test_publisher()}};
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    auto member = plexus::asio::shm::make_shm_member(io, broker);
    plexus::testing::bind_test_publisher(member);
    auto companion = member.mint_companion(fqn, nullptr);
    REQUIRE(companion);

    std::uint32_t refused = 0;
    companion->on_error([&refused](plexus::io::io_error) { ++refused; });
    const auto start = std::chrono::steady_clock::now();
    for(std::uint32_t sequence = 0; sequence < burst; ++sequence)
        companion->send(encode({0, sequence, false}));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    std::cout << "burst refused=" << refused << " of " << burst << " elapsed_ms=" << elapsed.count() << std::endl;

    REQUIRE(refused == 0);
}

TEST_CASE("lane_probe an unhinted pair that dials each other delivers each message exactly once", "[shm][lane_probe]")
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
