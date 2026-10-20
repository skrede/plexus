#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/hold_hook_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <atomic>
#include <vector>
#include <cstdint>
#include <optional>

namespace {

using plexus::shm::ring_role;
using plexus::shm::loan_status;
using plexus::shm::acquire_bound;
using plexus::shm::region_status;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using values        = std::vector<std::uint32_t>;
using hooked_holder = plexus::testing::basic_ring_holder<plexus::testing::hold_hook_broker<plexus::testing::native_broker>>;

constexpr std::uint64_t k_previous_ring = 0x50'4c'58'52'4e'47'34'00ull; // "PLXRNG4\0"

}

TEST_CASE("ring_lifetime a restarted publisher of another geometry retires the ring and its old reader is told", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("recreate")};
    ring_holder old_reader{bus, view.fqn};
    {
        ring_holder publisher{bus, view.fqn};
        REQUIRE(publisher.acquire(ring_role::producer, 64) == acquire_result::created);
        REQUIRE(old_reader.acquire(ring_role::consumer) == acquire_result::attached);
        publisher.release(ring_role::producer);
    }
    ring_holder restarted{bus, view.fqn};
#if defined(_WIN32)
    REQUIRE(restarted.acquire(ring_role::producer, 8192) == acquire_result::failed);
    CHECK(restarted.last_bound() == acquire_bound::incompatible);
    CHECK(old_reader.lost());
    old_reader.release(ring_role::consumer);
    REQUIRE(restarted.acquire(ring_role::producer, 8192) == acquire_result::created);
#else
    REQUIRE(restarted.acquire(ring_role::producer, 8192) == acquire_result::created);
    CHECK(old_reader.lost());
    old_reader.release(ring_role::consumer);
    REQUIRE(view.named());
#endif
    ring_holder newcomer{bus, view.fqn};
    REQUIRE(newcomer.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(restarted.send(9) == loan_status::ok);
    CHECK(newcomer.received() == values{9});
    CHECK(old_reader.received().empty());
    newcomer.release(ring_role::consumer);
    restarted.release(ring_role::producer);
    CHECK_FALSE(view.named());
}

TEST_CASE("ring_lifetime a publisher that died with a claim in flight is adopted and its reader receives every later message", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("in_flight")};
    ring_holder reader{bus, view.fqn};
    {
        ring_holder publisher{bus, view.fqn};
        REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
        REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
        REQUIRE(publisher.send(1) == loan_status::ok);
        REQUIRE(view.map());
        plexus::shm::broadcast_ring::claim_result claimed;
        REQUIRE(view.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable, plexus::io::congestion::block, claimed) == loan_status::ok);
        publisher.release(ring_role::producer);
    }
    ring_holder restarted{bus, view.fqn};
    REQUIRE(restarted.acquire(ring_role::producer) == acquire_result::attached);

    values expected{1};
    const auto last = static_cast<std::uint32_t>(view.ring.cell_count() + 2);
    for(std::uint32_t value = 2; value <= last; ++value)
    {
        REQUIRE(restarted.send(value) == loan_status::ok);
        expected.push_back(value);
    }
    CHECK(reader.received() == expected);
    reader.release(ring_role::consumer);
    restarted.release(ring_role::producer);
}

TEST_CASE("ring_lifetime a ring stamped by another layout is declined and its names survive", "[shm][ring_lifetime]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("foreign")};
    const plexus::shm::ring_geometry geom = plexus::shm::ring_geometry_for(64);
    REQUIRE(view.broker.create(view.name, plexus::shm::control_region_bytes(geom.cell_count), {}, view.control) == region_status::ok);
    REQUIRE(view.broker.create(view.name + ".s", plexus::shm::slab_region_bytes(geom.cell_count, geom.slot_capacity), {}, view.slab) == region_status::ok);
    REQUIRE(plexus::shm::broadcast_ring::create(view.control.bytes(), view.slab.bytes(), geom.cell_count, geom.slot_capacity, plexus::testing::test_publisher(), view.ring) ==
            loan_status::ok);
    view.ring.header().magic.store(k_previous_ring, std::memory_order_release);

    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::failed);
    CHECK(publisher.last_bound() == acquire_bound::incompatible);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::failed);
    CHECK(reader.last_bound() == acquire_bound::incompatible);
    CHECK(view.named());
    CHECK(view.ring.header().magic.load(std::memory_order_acquire) == k_previous_ring);
}

TEST_CASE("ring_lifetime a joiner that takes its slot after the ring was retired leaves as transient", "[shm][ring_lifetime]")
{
    const ring_role role = GENERATE(ring_role::producer, ring_role::consumer);
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_join")};
    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    if(role == ring_role::producer)
        publisher.release(ring_role::producer);
    REQUIRE(view.map());

    hooked_holder joiner{bus, view.fqn};
    joiner.broker().before_next_hold([&view] { view.ring.header().magic.store(plexus::shm::k_ring_retired, std::memory_order_release); });
    REQUIRE(joiner.acquire(role) == acquire_result::failed);
    CHECK(joiner.last_bound() == acquire_bound::transient_join);
    reader.release(ring_role::consumer);
    if(role == ring_role::consumer)
        publisher.release(ring_role::producer);
    CHECK(view.named());
}
