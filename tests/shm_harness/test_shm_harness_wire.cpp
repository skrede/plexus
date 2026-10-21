#include "node_config.h"
#include "topology_launch.h"

#include "support/region_names.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/upgrade_channel.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <string>
#include <cstdint>
#include <iostream>

using namespace shm_harness;

namespace {

constexpr std::uint64_t k_previous_ring = 0x50'4c'58'52'4e'47'35'00ull; // "PLXRNG5\0"

}

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

    // A send-only publisher arms no wake, so its ring stands and only the subscriber's denied wake keeps the pair on
    // the wire.
    REQUIRE(sub.received == 20);
    REQUIRE(sub.distinct == 20);
    REQUIRE(sub.duplicates == 0);
    REQUIRE(fallbacks_of(pub, plexus::io::fallback_reason::notifier_denied) == 0);
    REQUIRE(fallbacks_of(sub, plexus::io::fallback_reason::notifier_denied) >= 1);
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

// The region-observer oracle cannot tell the old region from a live ring, so the case asserts delivery and the counted
// reason instead.
TEST_CASE("harness a publisher whose ring name holds an older layout delivers every message over the wire", "[shm][harness]")
{
    const std::string fqn                 = "shm_harness.mixed." + std::to_string(plexus::testing::process_id());
    const std::string name                = plexus::testing::region_name(fqn, harness_id(0));
    const plexus::shm::ring_geometry geom = plexus::shm::ring_geometry_for(64);
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle control;
    plexus::native::shm_region_handle slab;
    const plexus::testing::unlink_on_exit names{control, slab};
    REQUIRE(broker.create(name, plexus::shm::control_region_bytes(geom.cell_count), {}, control) == plexus::shm::region_status::ok);
    REQUIRE(broker.create(name + ".s", plexus::shm::slab_region_bytes(geom.cell_count, geom.slot_capacity), {}, slab) == plexus::shm::region_status::ok);
    plexus::shm::broadcast_ring old_ring;
    REQUIRE(plexus::shm::broadcast_ring::create(control.bytes(), slab.bytes(), geom.cell_count, geom.slot_capacity, harness_id(0), old_ring) == plexus::shm::loan_status::ok);
    old_ring.header().magic.store(k_previous_ring, std::memory_order_release);

    topology topo{fqn, 2};
    REQUIRE(topo.created());
    REQUIRE(topo.spawn_node(0, node_role::pub, true, false));
    REQUIRE(topo.spawn_node(1, node_role::sub, true, false));
    REQUIRE(topo.wait_connected(10s));
    CHECK(topo.warm_up({0}, {1}, 10s));
    topo.publish_round({0}, 20, {1}, 10s);
    const node_counts pub = topo.counts(0);
    const node_counts sub = topo.counts(1);
    std::cout << "mixed " << summary(sub, true) << " publisher fallbacks=" << fallback_summary(pub) << std::endl;
    CHECK(topo.leave(1));
    CHECK(topo.leave(0));

    REQUIRE(sub.received == 20);
    REQUIRE(sub.distinct == 20);
    REQUIRE(sub.duplicates == 0);
    REQUIRE(fallbacks_of(pub, plexus::io::fallback_reason::incompatible_layout) >= 1);
    plexus::native::shm_region_handle probe;
    REQUIRE(broker.attach(name, probe) == plexus::shm::region_status::ok);
    REQUIRE(old_ring.header().magic.load(std::memory_order_acquire) == k_previous_ring);
}
