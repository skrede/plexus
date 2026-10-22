#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_RETIRE_STEPS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_RETIRE_STEPS_H

#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/ring_header.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdint>

namespace plexus::testing {

// The publisher leaves while the view holds a reader slot, so the retire falls to the view, as to any last holder.
inline void leave_retire_to(ring_view &retirer, wake_bus &bus)
{
    ring_holder publisher{bus, retirer.fqn};
    REQUIRE(publisher.acquire(shm::ring_role::producer) == shm::acquire_result::created);
    REQUIRE(retirer.map());
    REQUIRE(retirer.control.hold_slot(shm::cursor_slot(0)) == shm::slot_hold::held);
    publisher.release(shm::ring_role::producer);
    retirer.control.release_slot(shm::cursor_slot(0));
}

// ring_lifetime::close as far as the slab: every slot held, the ring retired, the slab's name removed.
inline void retire_holding(ring_view &retirer)
{
    REQUIRE(retirer.control.hold_all_slots() == shm::slot_hold::held);
    std::uint64_t found = shm::k_ring_magic;
    REQUIRE(retirer.ring.header().magic.compare_exchange_strong(found, shm::k_ring_retired));
    retirer.slab.unlink_name();
}

// The last holder dies after it removed the slab's name and before it removed the control name.
inline void crash_between_stamp_and_name(ring_view &holder, wake_bus &bus)
{
    leave_retire_to(holder, bus);
    retire_holding(holder);
    holder.unmap();
}

inline std::uint64_t named_identity(ring_view &view)
{
    native_broker::region_handle probe;
    REQUIRE(view.broker.attach(view.name, probe) == shm::region_status::ok);
    return shm::detail::ring_lifetime::identity_of(probe.bytes());
}

}

#endif
