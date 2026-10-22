#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/minter_entry.h"
#include "support/retire_steps.h"
#include "support/sidecar_probe.h"
#include "support/hold_hook_broker.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <string>
#include <vector>
#include <cstdint>

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
using plexus::testing::minter_entry;
using plexus::testing::canonical_of;
using plexus::testing::native_broker;
using plexus::testing::create_control;
using plexus::testing::release_minter;
using plexus::testing::named_identity;
using plexus::testing::sidecar_present;
using plexus::testing::crash_between_stamp_and_name;
using plexus::shm::detail::ring_lifetime;
using values        = std::vector<std::uint32_t>;
using hooked_holder = plexus::testing::basic_ring_holder<plexus::testing::hold_hook_broker<plexus::testing::native_broker>>;

// Where a sidecar lock file carries the slots, the successor's creator lock sits in the sidecar the late take meets.
constexpr acquire_bound k_late_take_bound = plexus::testing::k_sidecar_host ? acquire_bound::creator_alive : acquire_bound::transient_join;

// The name is removed and created afresh by another ring, which is retired in turn; its identity is returned.
std::uint64_t retire_successor(ring_view &view)
{
    const plexus::shm::ring_geometry geom = plexus::shm::ring_geometry_for(64);
    REQUIRE(view.broker.unlink(view.name) == region_status::ok);
    REQUIRE(view.broker.create(view.name, plexus::shm::control_region_bytes(geom.cell_count), {}, view.control) == region_status::ok);
    REQUIRE(view.broker.create(view.name + ".s", plexus::shm::slab_region_bytes(geom.cell_count, geom.slot_capacity), {}, view.slab) == region_status::ok);
    REQUIRE(plexus::shm::broadcast_ring::create(view.control.bytes(), view.slab.bytes(), geom.cell_count, geom.slot_capacity, plexus::testing::test_publisher(), view.ring) ==
            loan_status::ok);
    view.ring.header().magic.store(plexus::shm::k_ring_retired, std::memory_order_release);
    return view.ring.header().identity.load(std::memory_order_relaxed);
}

}

TEST_CASE("retired_name_identity a retired name left by a holder killed between its stamp and its control-name removal is removed by the next acquire, which engages",
          "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_name")};
    crash_between_stamp_and_name(view, bus);
    REQUIRE(view.named());

    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(publisher.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    CHECK(named_identity(view) == publisher.bound_identity());
    reader.release(ring_role::consumer);
    publisher.release(ring_role::producer);
    CHECK_FALSE(view.named());
    CHECK_FALSE(sidecar_present(canonical_of(view.name)));
}

TEST_CASE("retired_name_identity a retired name that now maps a successor ring is not removed", "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_successor")};
    crash_between_stamp_and_name(view, bus);
    std::uint64_t successor = 0;

    hooked_holder acquirer{bus, view.fqn};
    acquirer.broker().before_next_hold([&view, &successor] { successor = retire_successor(view); });
    REQUIRE(acquirer.acquire(ring_role::producer) == acquire_result::failed);
    CHECK(acquirer.last_bound() == acquire_bound::transient_join);
    REQUIRE(view.named());
    CHECK(successor != 0);
    CHECK(named_identity(view) == successor);
}

TEST_CASE("retired_name_identity a matching removal takes the name's sidecar with it", "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_sidecar")};
    const std::string canonical = canonical_of(view.name);
    crash_between_stamp_and_name(view, bus);
    REQUIRE(sidecar_present(canonical) == plexus::testing::k_sidecar_host);

    ring_holder reader{bus, view.fqn};
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::failed);
    CHECK(reader.last_bound() == acquire_bound::transient_join);
    CHECK_FALSE(view.named());
    CHECK_FALSE(sidecar_present(canonical));
}

TEST_CASE("retired_name_identity of two acquirers of a crash-left name one removes it and mints, the other is transient, and both then engage", "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_two")};
    crash_between_stamp_and_name(view, bus);

    ring_holder first{bus, view.fqn};
    hooked_holder second{bus, view.fqn};
    second.broker().before_next_hold([&first] { REQUIRE(first.acquire(ring_role::producer) == acquire_result::created); });
    REQUIRE(second.acquire(ring_role::consumer) == acquire_result::failed);
    CHECK(second.last_bound() == acquire_bound::transient_join);
    REQUIRE(second.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(first.send(3) == loan_status::ok);
    CHECK(second.received() == values{3});
    CHECK(named_identity(view) == first.bound_identity());
    second.release(ring_role::consumer);
    first.release(ring_role::producer);
    CHECK_FALSE(view.named());
}

TEST_CASE("retired_name_identity a retirer killed after it retired a ring it never stamped and before it removed the name leaves a name the next acquire clears",
          "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_unstamped")};
    native_broker broker;
    minter_entry retirer;
    create_control(broker, view.name, retirer);
    REQUIRE(ring_lifetime::take_creator(retirer) == acquire_bound::none);
    REQUIRE(ring_lifetime::stamp_retired(retirer, 0));
    release_minter(retirer);

    ring_holder publisher{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::attached);
    REQUIRE(publisher.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    reader.release(ring_role::consumer);
    publisher.release(ring_role::producer);
    CHECK_FALSE(view.named());
}

// The dead minter created the control region and left before its creator-slot take; the name is removed from outside
// the protocol, and another producer mints, between the adopter's attach and its take.
TEST_CASE("retired_name_identity an adopter whose dead minter's name another producer took over before its creator-slot take removes nothing",
          "[shm][retired_name_identity]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("retired_adopter")};
    minter_entry dead;
    create_control(view.broker, view.name, dead);
    release_minter(dead);

    ring_holder successor{bus, view.fqn};
    hooked_holder adopter{bus, view.fqn};
    adopter.broker().before_next_hold(
            [&view, &successor]
            {
                REQUIRE(view.broker.unlink(view.name) == region_status::ok);
                REQUIRE(successor.acquire(ring_role::producer) == acquire_result::created);
            });
    const acquire_result adopted = adopter.acquire(ring_role::producer);
    INFO("adopter bound " << static_cast<int>(adopter.last_bound()));
    CHECK(adopted == acquire_result::failed);
    CHECK(adopter.last_bound() == k_late_take_bound);
    CHECK(view.named());

    ring_holder reader{bus, view.fqn};
    CHECK(reader.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(successor.send(7) == loan_status::ok);
    CHECK(reader.received() == values{7});
    reader.release(ring_role::consumer);
    adopter.release(ring_role::producer);
    successor.release(ring_role::producer);
    CHECK_FALSE(view.named());
}
