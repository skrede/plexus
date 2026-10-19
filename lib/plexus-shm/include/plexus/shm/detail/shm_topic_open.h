#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H

#include "plexus/shm/detail/shm_topic_join.h"
#include "plexus/shm/detail/shm_topic_entry.h"
#include "plexus/shm/detail/shm_fallback_reason.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/same_host_geometry.h"

#include <string>
#include <cstdint>
#include <optional>

namespace plexus::shm::detail {

// Rejects an over-ceiling ring before any region is minted (a pure query, no broker touch),
// so an oversize declaration never orphans the control region.
inline bool topic_within_ceiling(const shm::ring_geometry &geom, std::uint64_t max_ring_slab_bytes, shm::acquire_failure &failure)
{
    const std::uint64_t ask = shm::slab_region_bytes(geom.cell_count, geom.slot_capacity);
    if(ask <= max_ring_slab_bytes)
        return true;
    failure = shm::acquire_failure{shm::acquire_bound::slab_ceiling, ask, max_ring_slab_bytes, shm::region_status::ok};
    return false;
}

// amode is the collision policy: reclaim_stale unlinks an existing name on create; join_live NEVER
// unlinks (it attaches first when the region exists, mints only when none does, so two co-host
// peers converge on the one ring).
template<typename Broker, typename Entry>
shm::acquire_result topic_attach_or_mint(Broker &broker, shm::acquire_failure &failure, Entry &e, const std::string &ctrl_name, const shm::ring_geometry &geom, const node_id &publisher,
                                         std::uint64_t capacity, shm::acquire_mode amode)
{
    const std::string slab_name = ctrl_name + ".s";
    if(amode == shm::acquire_mode::join_live && topic_region_exists(broker, ctrl_name))
        return topic_join(broker, failure, e, ctrl_name, slab_name, publisher);

    shm::create_options opts;
    opts.unlink_stale_on_create = amode == shm::acquire_mode::reclaim_stale;
    const shm::region_status cs = broker.create(ctrl_name, shm::control_region_bytes(geom.cell_count), opts, e.control);
    if(cs == shm::region_status::ok)
        return topic_mint(broker, failure, e, slab_name, geom, publisher, capacity);
    if(cs == shm::region_status::already_exists)
        return topic_join(broker, failure, e, ctrl_name, slab_name, publisher);
    failure = shm::acquire_failure{shm::acquire_bound::os_allocator, shm::control_region_bytes(geom.cell_count), 0, cs};
    return shm::acquire_result::failed;
}

// Mints or attaches the two regions of the key's publisher ring and binds the entry's ring.
template<typename Registry, typename Entry>
shm::acquire_result topic_open_ring(Registry &r, Entry &e, const shm_topic_key &key, std::uint32_t max_payload, io::ring_geometry_mode mode, std::uint32_t consumer_capacity,
                                    shm::acquire_mode amode)
{
    const std::string ctrl_name             = shm::region_name_for(key.fqn, key.publisher, r.m_universe, key.direction, r.m_region_ns);
    const std::optional<std::uint32_t> want = max_payload == 0 ? std::nullopt : std::optional<std::uint32_t>{max_payload};
    const std::uint64_t capacity            = consumer_capacity == 0 ? shm::k_max_consumers : consumer_capacity;
    const shm::ring_geometry geom           = shm::ring_geometry_for(want, mode, consumer_capacity);
    if(!topic_within_ceiling(geom, r.m_max_ring_slab_bytes, r.m_last_failure))
        return shm::acquire_result::failed;
    return topic_attach_or_mint(r.m_broker, r.m_last_failure, e, ctrl_name, geom, key.publisher, capacity, amode);
}

template<typename Report>
void topic_report(Report &report, io::fallback_reason reason)
{
    if(report)
        report(reason);
}

// A waiter lost after arming is reported once to the registry, then to every holder watching the entry; a notifier
// that moves to a lesser wake reports each move while it keeps serving the waiter.
template<typename Entry, typename Report>
shm::arm_status topic_arm(Entry &e, std::uint32_t slot, Report &report)
{
    using notifier_type = typename Entry::notifier_type;
    auto drain          = [&e] { e.deliver_pending(); };
    auto lost           = [loss = e.loss, &report](shm::arm_status status)
    {
        topic_report(report, io::fallback_reason::waiter_lost);
        loss->report(status);
    };
    if constexpr(shm::demotion_reporting_notifier<notifier_type>)
        return e.notify->arm(slot, drain, std::move(lost), [&report](shm::arm_status primary) { topic_report(report, detail::demoted_fallback(primary)); });
    else if constexpr(shm::demotable_notifier<notifier_type>)
        return e.notify->arm(slot, drain, std::move(lost));
    else
        return e.notify->arm(slot, drain);
}

// The channel must be built AFTER the ring is bound (its subscriber registers a cursor over the live ring). The armed
// drain is the ONE drain authority for the ring's single consumer cursor: a second drainer would steal messages. A
// subscriber the ring refused a cursor would never receive, so the bind fails rather than attach it.
template<typename Registry, typename Entry>
shm::acquire_failure topic_bind(Registry &r, Entry &e, const shm_topic_key &key, shm::acquire_result verdict)
{
    r.m_bind_notifier(e.notify, e.ring.header(), shm::region_name_for(key.fqn, key.publisher, r.m_universe, key.direction, r.m_region_ns));
    e.channel.emplace(e.ring, *e.notify, r.m_reliability, r.m_congestion);
    e.verdict                               = verdict;
    e.refcount                              = 1;
    const std::optional<std::uint32_t> slot = e.channel->waiter_slot();
    if(!slot)
        return shm::acquire_failure{shm::acquire_bound::consumer_capacity};
    if(const shm::arm_status armed = topic_arm(e, *slot, r.m_fallback_cb); armed != shm::arm_status::armed)
        return shm::acquire_failure{shm::acquire_bound::notifier, 0, 0, shm::region_status::ok, armed};
    return shm::acquire_failure{};
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
