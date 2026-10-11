#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"

#include <string>
#include <cstdint>
#include <optional>

namespace plexus::shm::detail {

// A successful read-only attach proves the region is live (the handle is discarded).
template<typename Registry>
bool topic_region_exists(Registry &r, const std::string &ctrl_name)
{
    typename Registry::broker_type::region_handle probe;
    return r.m_broker.attach(ctrl_name, probe) == shm::region_status::ok;
}

// The creator path: owns both regions, stamps a fresh ring, unlinks on teardown. The
// OS-allocator leg of the fail-closed gate names the OS bound on a slab-create failure.
template<typename Registry, typename Entry>
shm::acquire_result topic_mint(Registry &r, Entry &e, const std::string &slab_name, const shm::ring_geometry &geom, std::uint64_t consumer_capacity)
{
    const std::uint64_t ask = shm::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    if(const shm::region_status cs = r.m_broker.create(slab_name, ask, shm::create_options{.unlink_stale_on_create = true}, e.slab); cs != shm::region_status::ok)
    {
        r.m_last_failure = shm::acquire_failure{shm::acquire_bound::os_allocator, ask, 0, cs};
        return shm::acquire_result::failed;
    }
    if(shm::broadcast_ring::create(e.control.bytes(), e.slab.bytes(), geom.cell_count, geom.slot_capacity, e.ring, consumer_capacity) != shm::loan_status::ok)
        return shm::acquire_result::failed;
    e.creator = true;
    return shm::acquire_result::created;
}

// The attacher path: maps the peer's regions and re-reads the geometry from the control
// header (never unlinks).
template<typename Registry, typename Entry>
shm::acquire_result topic_join(Registry &r, Entry &e, const std::string &ctrl_name, const std::string &slab_name)
{
    if(r.m_broker.attach(ctrl_name, e.control) != shm::region_status::ok || r.m_broker.attach(slab_name, e.slab) != shm::region_status::ok)
        return shm::acquire_result::failed;
    if(shm::broadcast_ring::attach(e.control.bytes(), e.slab.bytes(), e.ring) != shm::loan_status::ok)
        return shm::acquire_result::failed;
    return shm::acquire_result::attached;
}

// Mints or attaches the two regions for (fqn, direction) and binds the entry's ring. amode is
// the collision policy: reclaim_stale unlinks an existing name on create; join_live NEVER
// unlinks (it attaches first when the region exists, mints only when none does, so two co-host
// peers converge on the one ring).
template<typename Registry, typename Entry>
// NOLINTNEXTLINE(readability-function-size)
shm::acquire_result topic_open_ring(Registry &r, Entry &e, const std::string &fqn, shm::ring_direction direction, std::uint32_t max_payload, shm::ring_geometry_mode mode,
                                    std::uint32_t consumer_capacity, shm::acquire_mode amode)
{
    const std::string ctrl_name             = shm::region_name_for(fqn, direction, r.m_region_ns);
    const std::string slab_name             = ctrl_name + ".s";
    const std::optional<std::uint32_t> want = max_payload == 0 ? std::nullopt : std::optional<std::uint32_t>{max_payload};
    const std::uint64_t capacity            = consumer_capacity == 0 ? shm::k_max_consumers : consumer_capacity;
    const shm::ring_geometry geom           = shm::ring_geometry_for(want, mode, consumer_capacity);

    // Reject an over-ceiling ring before any region is minted (a pure query, no broker touch),
    // so an oversize declaration never orphans the control region.
    const std::uint64_t ask = shm::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    if(ask > r.m_max_ring_slab_bytes)
    {
        r.m_last_failure = shm::acquire_failure{shm::acquire_bound::slab_ceiling, ask, r.m_max_ring_slab_bytes, shm::region_status::ok};
        return shm::acquire_result::failed;
    }

    if(amode == shm::acquire_mode::join_live && topic_region_exists(r, ctrl_name))
        return topic_join(r, e, ctrl_name, slab_name);

    shm::create_options opts;
    opts.unlink_stale_on_create = amode == shm::acquire_mode::reclaim_stale;
    const shm::region_status cs = r.m_broker.create(ctrl_name, shm::control_region_bytes(geom.cell_count), opts, e.control);
    if(cs == shm::region_status::ok)
        return topic_mint(r, e, slab_name, geom, capacity);
    if(cs == shm::region_status::already_exists)
        return topic_join(r, e, ctrl_name, slab_name);
    r.m_last_failure = shm::acquire_failure{shm::acquire_bound::os_allocator, shm::control_region_bytes(geom.cell_count), 0, cs};
    return shm::acquire_result::failed;
}

// A notifier that can lose its waiter after arming reports the loss to every holder watching the entry.
template<typename Entry>
shm::arm_status topic_arm(Entry &e, std::uint32_t slot)
{
    if constexpr(shm::demotable_notifier<typename Entry::notifier_type>)
        return e.notify->arm(slot, [&e] { e.deliver_pending(); }, [loss = e.loss](shm::arm_status status) { loss->report(status); });
    else
        return e.notify->arm(slot, [&e] { e.deliver_pending(); });
}

// The channel must be built AFTER the ring is bound (its subscriber registers a cursor over the live ring). The armed
// drain is the ONE drain authority for the ring's single consumer cursor: a second drainer would steal messages. A
// waiter without a cursor slot has nothing to wake and reports armed.
template<typename Registry, typename Entry>
shm::arm_status topic_bind(Registry &r, Entry &e, const std::string &fqn, shm::ring_direction direction, shm::acquire_result verdict)
{
    r.m_bind_notifier(e.notify, e.ring.header(), shm::region_name_for(fqn, direction, r.m_region_ns));
    e.channel.emplace(e.ring, *e.notify, r.m_reliability, r.m_congestion);
    e.verdict  = verdict;
    e.refcount = 1;
    if(const std::optional<std::uint32_t> slot = e.channel->waiter_slot())
        return topic_arm(e, *slot);
    return shm::arm_status::armed;
}

// A key whose waiter was lost fails every acquire until its last holder releases it, so the next demand arms afresh.
template<typename Entry>
shm::acquire_result topic_reacquire(Entry &e, shm::acquire_failure &failure)
{
    if(const shm::arm_status lost = e.loss->status(); lost != shm::arm_status::armed)
    {
        failure = shm::acquire_failure{shm::acquire_bound::notifier, 0, 0, shm::region_status::ok, lost};
        return shm::acquire_result::failed;
    }
    ++e.refcount;
    return e.verdict;
}

}

#endif
