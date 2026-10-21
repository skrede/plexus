#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/shm_acquire_result.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstdint>

namespace {

using plexus::shm::ring_role;
using plexus::shm::slot_hold;
using plexus::shm::cursor_slot;
using plexus::shm::loan_status;
using plexus::shm::acquire_result;
using plexus::testing::wake_bus;
using plexus::testing::ring_view;
using plexus::testing::ring_holder;
using plexus::testing::lifetime_fqn;
using values = std::vector<std::uint32_t>;

// The view becomes the ring's last holder, retires it as close() does and removes the slab's name, then dies before it
// removes the control name.
void crash_between_stamp_and_name(ring_view &holder, wake_bus &bus)
{
    {
        ring_holder publisher{bus, holder.fqn};
        REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
        REQUIRE(holder.map());
        REQUIRE(holder.control.hold_slot(cursor_slot(0)) == slot_hold::held);
        publisher.release(ring_role::producer);
    }
    holder.control.release_slot(cursor_slot(0));
    REQUIRE(holder.control.hold_all_slots() == slot_hold::held);
    std::uint64_t found = plexus::shm::k_ring_magic;
    REQUIRE(holder.ring.header().magic.compare_exchange_strong(found, plexus::shm::k_ring_retired));
    holder.slab.unlink_name();
    holder.unmap();
}

}

TEST_CASE("retired_name_identity a retired name left by a holder killed between its stamp and its control-name removal is removed by the next acquire, which engages",
          "[shm][retired_name_identity][!shouldfail]")
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
    reader.release(ring_role::consumer);
    publisher.release(ring_role::producer);
    CHECK_FALSE(view.named());
}
