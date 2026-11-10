#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/minter_entry.h"
#include "support/sidecar_probe.h"
#include "support/hold_hook_broker.h"

#include "plexus/shm/detail/shm_ring_names.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstdint>

namespace {

using plexus::shm::ring_role;
using plexus::shm::loan_status;
using plexus::shm::region_status;
using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::shm::acquire_failure;
using plexus::testing::wake_bus;
using plexus::testing::mint_held;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using plexus::testing::minter_entry;
using plexus::testing::native_broker;
using plexus::testing::create_control;
using plexus::testing::release_minter;
using plexus::shm::detail::ring_names;
using values        = std::vector<std::uint32_t>;
using hooked_holder = plexus::testing::basic_ring_holder<plexus::testing::hold_hook_broker<native_broker>>;

// Where a sidecar lock file carries the slots, the adopter's new region shares the path's sidecar, so its creator lock
// refuses the minter the slot; where the region's own locks carry them, the minter takes the retired region's slot.
constexpr acquire_bound k_minter_bound = plexus::testing::k_sidecar_host ? acquire_bound::creator_alive : acquire_bound::transient_join;

// Where a sidecar lock file carries the slots, the successor's creator lock refuses the late take before any slab create.
constexpr acquire_bound k_failed_mint_bound = plexus::testing::k_sidecar_host ? acquire_bound::creator_alive : acquire_bound::os_allocator;

}

// The producer's mint, driven step by step: its exclusive create of the control region, then a second producer's whole
// acquire, then the first one's creator-slot take and slab mint.
TEST_CASE("mint_takeover a minter whose unstamped ring an adopter recreated leaves the adopter's ring whole", "[shm][mint_takeover]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("mint_takeover")};
    native_broker broker;
    minter_entry minter;
    create_control(broker, view.name, minter);

    ring_holder adopter{bus, view.fqn};
    REQUIRE(adopter.acquire(ring_role::producer) == acquire_result::created);
    acquire_failure failure{};
    const acquire_result minted = mint_held(broker, minter, view.name, failure);

    ring_holder reader{bus, view.fqn};
    CHECK(reader.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(adopter.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    CHECK(minted == acquire_result::failed);
    CHECK(failure.bound == k_minter_bound);
    reader.release(ring_role::consumer);
    adopter.release(ring_role::producer);
    release_minter(minter);
}

// The take falls between the adopter's release of the retired region's creator slot and its take on the new region.
TEST_CASE("mint_takeover a minter that takes its creator slot after an adopter retired its region and removed its name declines before making a slab", "[shm][mint_takeover]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("mint_late_take")};
    native_broker broker;
    minter_entry minter;
    create_control(broker, view.name, minter);
    plexus::testing::retire_named(broker, view.name, 0);

    acquire_failure failure{};
    CHECK(mint_held(broker, minter, view.name, failure) == acquire_result::failed);
    CHECK(failure.bound == acquire_bound::transient_join);
    CHECK(ring_names::magic_of(minter.control.bytes()) == plexus::shm::k_ring_retired);
    release_minter(minter);
    plexus::testing::engage_afresh(bus, view);
}

// The control name is removed from outside the protocol, which leaves the first holder as the takeover would.
TEST_CASE("mint_takeover a holder whose control name another producer created anew closes without removing that producer's names", "[shm][mint_takeover]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("mint_close")};
    ring_holder first{bus, view.fqn};
    REQUIRE(first.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(view.broker.unlink(view.name) == region_status::ok);
    ring_holder second{bus, view.fqn};
    REQUIRE(second.acquire(ring_role::producer) == acquire_result::created);
    first.release(ring_role::producer);

    ring_holder reader{bus, view.fqn};
    CHECK(reader.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(second.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    CHECK(view.named());
    reader.release(ring_role::consumer);
    second.release(ring_role::producer);
}

// The control name is removed from outside the protocol, and another producer mints, between the minter's exclusive
// create and its creator-slot take; the slab create that fails is the test broker's.
TEST_CASE("mint_takeover a minter whose control name another producer took over before its creator-slot take, and whose slab create then fails, removes nothing",
          "[shm][mint_takeover]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("mint_failed")};
    ring_holder successor{bus, view.fqn};
    hooked_holder minter{bus, view.fqn};
    minter.broker().fail_next_create(view.name + ".s", region_status::no_space);
    minter.broker().before_next_hold(
            [&view, &successor]
            {
                REQUIRE(view.broker.unlink(view.name) == region_status::ok);
                REQUIRE(successor.acquire(ring_role::producer) == acquire_result::created);
            });
    REQUIRE(minter.acquire(ring_role::producer) == acquire_result::failed);
    CHECK(minter.last_bound() == k_failed_mint_bound);
    CHECK(view.named());

    ring_holder reader{bus, view.fqn};
    CHECK(reader.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(successor.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    reader.release(ring_role::consumer);
    successor.release(ring_role::producer);
    CHECK_FALSE(view.named());
}
