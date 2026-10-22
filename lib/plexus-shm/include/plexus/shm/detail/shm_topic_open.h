#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_OPEN_H

#include "plexus/shm/detail/shm_topic_join.h"
#include "plexus/shm/detail/shm_topic_entry.h"
#include "plexus/shm/detail/shm_ring_adoption.h"
#include "plexus/shm/detail/shm_ring_lifetime.h"
#include "plexus/shm/detail/shm_fallback_reason.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/same_host_geometry.h"

#include <atomic>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace plexus::shm::detail {

// Rejects an over-ceiling ring before any region is minted (a pure query, no broker touch),
// so an oversize declaration never orphans the control region. The ceiling bounds the slot area, not the slab header.
inline bool topic_within_ceiling(const shm::ring_geometry &geom, std::uint64_t max_ring_slab_bytes, shm::acquire_failure &failure)
{
    const std::uint64_t ask = shm::slab_payload_bytes(geom.cell_count, geom.slot_capacity);
    if(ask <= max_ring_slab_bytes)
        return true;
    failure = shm::acquire_failure{shm::acquire_bound::slab_ceiling, ask, max_ring_slab_bytes, shm::region_status::ok};
    return false;
}

// Mints, adopts or attaches the two regions of the key's publisher ring and binds the entry's ring; a consumer only
// attaches.
template<typename Registry, typename Entry>
shm::acquire_result topic_open_ring(Registry &r, Entry &e, const shm_topic_key &key, shm::ring_role role, std::uint32_t max_payload, io::ring_geometry_mode mode,
                                    std::uint32_t consumer_capacity)
{
    e.ctrl_name = shm::region_name_for(key.fqn, key.publisher, r.m_universe, key.direction, r.m_region_ns);
    if(role == shm::ring_role::consumer)
        return topic_join(r.m_broker, r.m_last_failure, e, e.ctrl_name, e.ctrl_name + ".s", key.publisher);
    const std::optional<std::uint32_t> want = max_payload == 0 ? std::nullopt : std::optional<std::uint32_t>{max_payload};
    const std::uint64_t capacity            = consumer_capacity == 0 ? shm::k_max_consumers : consumer_capacity;
    const shm::ring_geometry geom           = shm::ring_geometry_for(want, mode, consumer_capacity);
    if(!topic_within_ceiling(geom, r.m_max_ring_slab_bytes, r.m_last_failure))
        return shm::acquire_result::failed;
    return ring_adoption::open_producer(r, e, e.ctrl_name, geom, key.publisher, capacity);
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
    auto drain          = [&e, &report] { ring_lifetime::drain(e, report); };
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

enum class cursor_claim : std::uint8_t
{
    claimed,
    passing,
    taken,
    unprovable,
    retired,
};

// An active cursor whose slot no other holder has belongs to a dead owner, since an owner holds the slot from before its
// claim until after its release; that cursor is reclaimed for this reader.
template<typename Entry>
std::optional<std::uint32_t> topic_claim_or_reclaim(Entry &e, std::uint32_t i)
{
    auto owner_alive = [&control = e.control](std::uint32_t cursor) { return control.slot_held_elsewhere(shm::cursor_slot(cursor)); };
    if(const std::optional<std::uint32_t> generation = e.ring.claim_cursor(i))
        return generation;
    return e.ring.reclaim_cursor(i, owner_alive) ? e.ring.claim_cursor(i) : std::nullopt;
}

// A slot held elsewhere over an inactive cursor is a holder passing through, such as a last holder taking every slot for
// a moment.
template<typename Entry>
cursor_claim topic_try_cursor(Entry &e, std::uint32_t i, bool held)
{
    const shm::slot_hold hold = e.control.hold_slot(shm::cursor_slot(i));
    if(hold == shm::slot_hold::unsupported || hold == shm::slot_hold::failed)
        return cursor_claim::unprovable;
    if(hold == shm::slot_hold::busy)
        return e.ring.header().cursors[i].active.load(std::memory_order_acquire) == 0 ? cursor_claim::passing : cursor_claim::taken;
    const bool retired                            = ring_lifetime::retired(e.ring);
    const std::optional<std::uint32_t> generation = retired ? std::nullopt : topic_claim_or_reclaim(e, i);
    if(!generation)
    {
        e.control.release_slot(shm::cursor_slot(i));
        return retired ? cursor_claim::retired : cursor_claim::taken;
    }
    if(held)
        e.channel->engage_subscriber(shm::slot_subscriber::hold_cursor, i, *generation, e.mapping);
    else
        e.channel->engage_subscriber(shm::slot_subscriber::adopt_cursor, i, *generation, e.mapping);
    return cursor_claim::claimed;
}

// A held cursor gates nothing until its reader is placed or resumed. Only a full cursor table is consumer_capacity.
template<typename Entry>
shm::acquire_failure topic_claim_cursor(Entry &e, bool held)
{
    const std::uint64_t capacity = std::min<std::uint64_t>(e.ring.header().consumer_capacity, shm::k_max_consumers);
    bool passing                 = false;
    for(std::uint32_t i = 0; i < capacity; ++i)
    {
        switch(topic_try_cursor(e, i, held))
        {
            case cursor_claim::claimed:
                return shm::acquire_failure{};
            case cursor_claim::unprovable:
                return shm::acquire_failure{shm::acquire_bound::liveness};
            case cursor_claim::retired:
                return shm::acquire_failure{shm::acquire_bound::transient_join};
            case cursor_claim::passing:
                passing = true;
                break;
            case cursor_claim::taken:
                break;
        }
    }
    return shm::acquire_failure{passing ? shm::acquire_bound::transient_join : shm::acquire_bound::consumer_capacity};
}

// A subscriber the ring refused a cursor would never receive, so the engagement fails rather than attach it; the caller
// undoes a failed engagement.
template<typename Entry, typename Report>
shm::acquire_failure topic_engage_consumer(Entry &e, Report &report, bool held)
{
    if(const shm::acquire_failure claimed = topic_claim_cursor(e, held); claimed.bound != shm::acquire_bound::none)
        return claimed;
    if(const shm::arm_status armed = topic_arm(e, *e.channel->waiter_slot(), report); armed != shm::arm_status::armed)
        return shm::acquire_failure{shm::acquire_bound::notifier, 0, 0, shm::region_status::ok, armed};
    return shm::acquire_failure{};
}

// The channel must be built AFTER the ring is bound. Only a consuming role registers the cursor and arms; the armed drain
// is the one drainer of that cursor, since a second would steal its messages.
template<typename Registry, typename Entry>
shm::acquire_failure topic_bind(Registry &r, Entry &e, shm::ring_role role, shm::acquire_result verdict)
{
    r.m_bind_notifier(e.notify, e.ring.header(), e.ctrl_name);
    e.channel.emplace(e.ring, *e.notify, r.m_reliability, r.m_congestion);
    e.channel->bind_cursor_liveness([&control = e.control](std::uint32_t cursor) { return control.slot_held_elsewhere(shm::cursor_slot(cursor)); });
    e.verdict = verdict;
    e.hold(role);
    if(role == shm::ring_role::producer)
        return shm::acquire_failure{};
    return topic_engage_consumer(e, r.m_fallback_cb, role == shm::ring_role::consumer);
}

}

#endif
