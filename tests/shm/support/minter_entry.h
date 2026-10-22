#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_MINTER_ENTRY_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_MINTER_ENTRY_H

#include "support/bus_notifier.h"
#include "support/lifetime_rig.h"
#include "support/region_names.h"

#include "plexus/shm/detail/shm_topic_join.h"
#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/same_host_geometry.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>

namespace plexus::testing {

// The parts of a registry entry a producer's open touches, so a test takes the open's steps through the same verbs.
struct minter_entry
{
    native_broker::region_handle control;
    native_broker::region_handle slab;
    shm::broadcast_ring ring;
    bool creator = false;
};

inline shm::ring_geometry minter_geometry()
{
    return shm::ring_geometry_for(64, io::ring_geometry_mode::reliable_preserving, 0);
}

inline void create_control(native_broker &broker, const std::string &name, minter_entry &e)
{
    REQUIRE(broker.create(name, shm::control_region_bytes(minter_geometry().cell_count), shm::create_options{}, e.control) == shm::region_status::ok);
}

// The mint as ring_adoption::mint takes it: the creator slot, then the slab and the stamp.
inline shm::acquire_result mint_held(native_broker &broker, minter_entry &e, const std::string &name, shm::acquire_failure &failure)
{
    if(const shm::acquire_bound took = shm::detail::ring_lifetime::take_creator(e); took != shm::acquire_bound::none)
    {
        failure = shm::acquire_failure{took};
        return shm::acquire_result::failed;
    }
    return shm::detail::topic_mint(broker, failure, e, name + ".s", minter_geometry(), test_publisher(), shm::k_max_consumers);
}

inline void release_minter(minter_entry &e)
{
    shm::detail::ring_lifetime::drop_creator(e);
    e.slab    = native_broker::region_handle{};
    e.control = native_broker::region_handle{};
}

// Another producer's retire of the ring under the name, up to its release of the creator slot: the stamp over the magic
// it found, then the slab name and the control name.
inline void retire_named(native_broker &broker, const std::string &name, std::uint64_t found)
{
    minter_entry other;
    REQUIRE(broker.attach(name, other.control) == shm::region_status::ok);
    if(found == shm::k_ring_magic)
        REQUIRE(broker.attach(name + ".s", other.slab) == shm::region_status::ok);
    REQUIRE(shm::detail::ring_lifetime::take_creator(other) == shm::acquire_bound::none);
    REQUIRE(shm::detail::ring_lifetime::stamp_retired(other, found));
    other.slab.unlink_name();
    other.control.unlink_name();
    release_minter(other);
}

// A producer and a reader that come once the minter has left engage a ring of their own and leave no name behind.
inline void engage_afresh(wake_bus &bus, ring_view &view)
{
    ring_holder producer{bus, view.fqn};
    ring_holder reader{bus, view.fqn};
    REQUIRE(producer.acquire(shm::ring_role::producer) == shm::acquire_result::created);
    REQUIRE(reader.acquire(shm::ring_role::consumer) == shm::acquire_result::attached);
    REQUIRE(producer.send(7) == shm::loan_status::ok);
    CHECK(reader.received() == std::vector<std::uint32_t>{7});
    reader.release(shm::ring_role::consumer);
    producer.release(shm::ring_role::producer);
    CHECK_FALSE(view.named());
}

}

#endif
