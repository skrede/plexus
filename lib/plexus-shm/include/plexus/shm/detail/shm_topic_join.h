#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_JOIN_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_JOIN_H

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/node_id.h"

#include <string>
#include <cstdint>

namespace plexus::shm::detail {

// A successful read-only attach proves the region is live (the handle is discarded).
template<typename Broker>
bool topic_region_exists(Broker &broker, const std::string &ctrl_name)
{
    typename Broker::region_handle probe;
    return broker.attach(ctrl_name, probe) == shm::region_status::ok;
}

// The creator path: owns both regions, stamps a fresh ring, unlinks on teardown. The
// OS-allocator leg of the fail-closed gate names the OS bound on a slab-create failure.
template<typename Broker, typename Entry>
shm::acquire_result topic_mint(Broker &broker, shm::acquire_failure &failure, Entry &e, const std::string &slab_name, const shm::ring_geometry &geom, const node_id &publisher,
                               std::uint64_t consumer_capacity)
{
    const std::uint64_t ask = shm::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    if(const shm::region_status cs = broker.create(slab_name, ask, shm::create_options{.unlink_stale_on_create = true}, e.slab); cs != shm::region_status::ok)
    {
        failure = shm::acquire_failure{shm::acquire_bound::os_allocator, ask, 0, cs};
        return shm::acquire_result::failed;
    }
    if(shm::broadcast_ring::create(e.control.bytes(), e.slab.bytes(), geom.cell_count, geom.slot_capacity, publisher, e.ring, consumer_capacity) != shm::loan_status::ok)
        return shm::acquire_result::failed;
    e.creator = true;
    return shm::acquire_result::created;
}

// The attacher path: maps the peer's regions and re-reads the geometry from the control header (never unlinks). A slab
// missing beside a live control region, or a header not stamped yet, is a creator still minting: a transient join. A
// header another publisher owns is incompatible.
template<typename Broker, typename Entry>
shm::acquire_result topic_join(Broker &broker, shm::acquire_failure &failure, Entry &e, const std::string &ctrl_name, const std::string &slab_name, const node_id &publisher)
{
    const auto fail = [&failure](shm::acquire_bound bound, shm::region_status status = shm::region_status::ok)
    {
        failure = shm::acquire_failure{bound, 0, 0, status};
        return shm::acquire_result::failed;
    };
    if(const shm::region_status cs = broker.attach(ctrl_name, e.control); cs != shm::region_status::ok)
        return fail(shm::acquire_bound::os_allocator, cs);
    if(const shm::region_status ss = broker.attach(slab_name, e.slab); ss != shm::region_status::ok)
        return fail(ss == shm::region_status::not_found ? shm::acquire_bound::transient_join : shm::acquire_bound::os_allocator, ss);
    switch(shm::broadcast_ring::attach(e.control.bytes(), e.slab.bytes(), publisher, e.ring))
    {
        case shm::loan_status::ok:
            return shm::acquire_result::attached;
        case shm::loan_status::unstamped:
            return fail(shm::acquire_bound::transient_join);
        default:
            return fail(shm::acquire_bound::incompatible);
    }
}

}

#endif
