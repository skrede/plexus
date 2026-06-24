#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_acquire_result.h"

#include <string>
#include <cstdint>
#include <optional>

// The family's own detail namespace: the registry body reaches these helpers through a bare
// detail:: lookup that resolves here by fall-through from plexus::shm.
namespace plexus::shm::detail {

// A cheap existence probe for the consumer attach-first path: a successful read-only attach
// proves the region is live (the handle is discarded). A miss returns false (the consumer
// then mints).
template<typename Registry>
bool topic_region_exists(Registry &r, const std::string &ctrl_name)
{
    typename Registry::broker_type::region_handle probe;
    return r.m_broker.attach(ctrl_name, probe) == shm::region_status::ok;
}

// The creator path: it owns both regions, stamps a fresh ring at the resolved consumer
// capacity, and unlinks on teardown. The OS-allocator leg of the fail-closed gate (the
// ceiling was already cleared in topic_open_ring) names the OS bound on a slab-create failure.
template<typename Registry, typename Entry>
shm::acquire_result topic_mint(Registry &r, Entry &e, const std::string &slab_name,
                               const shm::ring_geometry &geom, std::uint64_t consumer_capacity)
{
    const std::uint64_t ask = shm::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    if(const shm::region_status cs = r.m_broker.create(
               slab_name, ask, shm::create_options{.unlink_stale_on_create = true}, e.slab);
       cs != shm::region_status::ok)
    {
        r.m_last_failure = shm::acquire_failure{shm::acquire_bound::os_allocator, ask, 0, cs};
        return shm::acquire_result::failed;
    }
    if(shm::broadcast_ring::create(e.control.bytes(), e.slab.bytes(), geom.cell_count,
                                   geom.slot_capacity, e.ring,
                                   consumer_capacity) != shm::loan_status::ok)
        return shm::acquire_result::failed;
    e.creator = true;
    return shm::acquire_result::created;
}

// The attacher path: maps the peer's existing regions and re-reads the geometry from the
// control header (never unlinks).
template<typename Registry, typename Entry>
shm::acquire_result topic_join(Registry &r, Entry &e, const std::string &ctrl_name,
                               const std::string &slab_name)
{
    if(r.m_broker.attach(ctrl_name, e.control) != shm::region_status::ok ||
       r.m_broker.attach(slab_name, e.slab) != shm::region_status::ok)
        return shm::acquire_result::failed;
    if(shm::broadcast_ring::attach(e.control.bytes(), e.slab.bytes(), e.ring) !=
       shm::loan_status::ok)
        return shm::acquire_result::failed;
    return shm::acquire_result::attached;
}

// Mints or attaches the two regions for (fqn, direction) and binds the entry's ring over them.
// amode decides the collision policy: reclaim_stale unlinks an existing name on create (the
// single-owner dial ring reclaims a crashed creator's orphan); join_live NEVER unlinks (a live
// co-host peer already mapped this companion ring) — it attaches FIRST when the region exists,
// and mints only when none does yet, so the peer arriving second JOINs and both converge.
template<typename Registry, typename Entry>
// NOLINTNEXTLINE(readability-function-size)
shm::acquire_result topic_open_ring(Registry &r, Entry &e, const std::string &fqn,
                                    shm::ring_direction direction, std::uint32_t max_payload,
                                    shm::ring_geometry_mode mode, std::uint32_t consumer_capacity,
                                    shm::acquire_mode amode)
{
    const std::string ctrl_name = shm::region_name_for(fqn, direction, r.m_region_ns);
    const std::string slab_name = ctrl_name + ".s";
    const std::optional<std::uint32_t> want =
            max_payload == 0 ? std::nullopt : std::optional<std::uint32_t>{max_payload};
    const std::uint64_t capacity =
            consumer_capacity == 0 ? shm::k_max_consumers : consumer_capacity;
    const shm::ring_geometry geom = shm::ring_geometry_for(want, mode, consumer_capacity);

    // The ceiling leg of the fail-closed gate is a PURE query (no broker touch): reject an
    // over-ceiling reliable ring before any region is minted, so an oversize declaration never
    // orphans the control region. The ask is the slab the ring would size (it dominates).
    const std::uint64_t ask = shm::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    if(ask > r.m_max_ring_slab_bytes)
    {
        r.m_last_failure = shm::acquire_failure{shm::acquire_bound::slab_ceiling, ask,
                                                r.m_max_ring_slab_bytes, shm::region_status::ok};
        return shm::acquire_result::failed;
    }

    if(amode == shm::acquire_mode::join_live && topic_region_exists(r, ctrl_name))
        return topic_join(r, e, ctrl_name, slab_name);

    shm::create_options opts;
    opts.unlink_stale_on_create = amode == shm::acquire_mode::reclaim_stale;
    const shm::region_status cs = r.m_broker.create(
            ctrl_name, shm::control_region_bytes(geom.cell_count), opts, e.control);
    if(cs == shm::region_status::ok)
        return topic_mint(r, e, slab_name, geom, capacity);
    if(cs == shm::region_status::already_exists)
        return topic_join(r, e, ctrl_name, slab_name);
    r.m_last_failure = shm::acquire_failure{shm::acquire_bound::os_allocator,
                                            shm::control_region_bytes(geom.cell_count), 0, cs};
    return shm::acquire_result::failed;
}

}

#endif
