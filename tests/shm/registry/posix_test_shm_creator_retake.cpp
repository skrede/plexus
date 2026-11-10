#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/minter_entry.h"

#include "plexus/shm/detail/shm_ring_names.h"
#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/ring_header.h"
#include "plexus/shm/shm_acquire_result.h"

#include <catch2/catch_test_macros.hpp>

namespace {

using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::shm::acquire_failure;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::lifetime_fqn;
using plexus::testing::minter_entry;
using plexus::testing::native_broker;
using plexus::testing::release_minter;
using plexus::shm::detail::ring_names;
using plexus::shm::detail::ring_lifetime;

}

// The entry keeps its handles once its last producer left, as a consumer still reading keeps it; the retire falls
// between topic_rejoin's retired check and its retake of the creator slot.
TEST_CASE("creator_retake a producer returning to a ring another producer retired meanwhile declines the creator slot", "[shm][creator_retake]")
{
    wake_bus bus;
    ring_view view{lifetime_fqn("creator_retake")};
    native_broker broker;
    minter_entry kept;
    plexus::testing::create_control(broker, view.name, kept);
    acquire_failure failure{};
    REQUIRE(plexus::testing::mint_held(broker, kept, view.name, failure) == acquire_result::created);
    ring_lifetime::drop_creator(kept);
    plexus::testing::retire_named(broker, view.name, plexus::shm::k_ring_magic);

    CHECK(ring_lifetime::retake_creator(kept) == acquire_bound::transient_join);
    CHECK_FALSE(kept.creator);
    CHECK(ring_names::magic_of(kept.control.bytes()) == plexus::shm::k_ring_retired);
    release_minter(kept);
    plexus::testing::engage_afresh(bus, view);
}
