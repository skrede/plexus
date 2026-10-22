#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_ADOPTION_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_ADOPTION_H

#include "plexus/shm/detail/shm_ring_names.h"
#include "plexus/shm/detail/shm_topic_join.h"
#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/node_id.h"

#include <string>
#include <cstdint>
#include <optional>

namespace plexus::shm::detail {

// A producer's open: a fresh name is minted, a live ring of this publisher is adopted in place under its creator slot,
// and a ring of another geometry, or one its dead creator never stamped, is retired and created afresh.
struct ring_adoption
{
    template<typename Registry, typename Entry>
    static acquire_result open_producer(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity)
    {
        const region_status cs = r.m_broker.create(ctrl_name, control_region_bytes(geom.cell_count), create_options{}, e.control);
        if(cs == region_status::ok)
            return mint(r, e, ctrl_name, geom, publisher, capacity);
        if(cs == region_status::already_exists)
            return adopt(r, e, ctrl_name, geom, publisher, capacity);
        return fail(r, acquire_failure{acquire_bound::os_allocator, control_region_bytes(geom.cell_count), 0, cs});
    }

private:
    enum class fit : std::uint8_t
    {
        same,
        other_geometry,
        foreign,
    };

    template<typename Registry>
    static acquire_result fail(Registry &r, acquire_failure failure) noexcept
    {
        r.m_last_failure = failure;
        return acquire_result::failed;
    }

    // A mint that fails after its fresh create retires the name it created.
    template<typename Registry, typename Entry>
    static acquire_result mint(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity)
    {
        if(const acquire_bound held = ring_lifetime::take_creator(e); held != acquire_bound::none)
            return fail(r, acquire_failure{held});
        const acquire_result minted = topic_mint(r.m_broker, r.m_last_failure, e, ctrl_name + ".s", geom, publisher, capacity);
        if(minted == acquire_result::failed && ring_lifetime::stamp_retired(e, 0))
            ring_lifetime::unlink(e);
        return minted;
    }

    // A magic this build does not know is declined before any slot is taken: its slots prove nothing about its holders.
    template<typename Registry, typename Entry>
    static acquire_result adopt(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity)
    {
        if(const region_status as = r.m_broker.attach(ctrl_name, e.control); as != region_status::ok)
            return fail(r, acquire_failure{as == region_status::not_found ? acquire_bound::transient_join : acquire_bound::os_allocator, 0, 0, as});
        if(!ours(ring_names::magic_of(e.control.bytes())))
            return fail(r, acquire_failure{acquire_bound::incompatible});
        if(const acquire_bound held = ring_lifetime::take_creator(e); held != acquire_bound::none)
            return fail(r, acquire_failure{held});
        return adopt_held(r, e, ctrl_name, geom, publisher, capacity);
    }

    static bool ours(std::uint64_t magic) noexcept
    {
        return magic == 0 || magic == k_ring_magic || magic == k_ring_retired;
    }

    // The creator slot is held, so the magic read here is final: no live creator stamps it and no other holder retires it.
    template<typename Registry, typename Entry>
    static acquire_result adopt_held(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity)
    {
        const std::uint64_t magic = ring_names::magic_of(e.control.bytes());
        if(magic == k_ring_retired)
            return reclaim_retired(r, e, ctrl_name, geom, publisher, capacity);
        if(magic == 0)
            return recreate(r, e, ctrl_name, geom, publisher, capacity, 0);
        if(magic != k_ring_magic)
            return fail(r, acquire_failure{acquire_bound::incompatible});
        const fit found = attach_existing(r, e, ctrl_name, geom, publisher, capacity);
        if(found == fit::foreign)
            return fail(r, acquire_failure{acquire_bound::incompatible});
        if(found == fit::other_geometry)
            return recreate(r, e, ctrl_name, geom, publisher, capacity, k_ring_magic);
        ring_lifetime::repair(e.ring);
        return acquire_result::attached;
    }

    // A slab missing under a stamped ring is one a dead holder was retiring, so that ring is recreated too.
    template<typename Registry, typename Entry>
    static fit attach_existing(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity)
    {
        if(const region_status ss = r.m_broker.attach(ctrl_name + ".s", e.slab); ss != region_status::ok)
            return ss == region_status::not_found ? fit::other_geometry : fit::foreign;
        if(broadcast_ring::attach(e.control.bytes(), e.slab.bytes(), publisher, e.ring) != loan_status::ok)
            return fit::foreign;
        const control_header_t &h = e.ring.header();
        return h.cell_count == geom.cell_count && h.slot_capacity == geom.slot_capacity && h.consumer_capacity == capacity ? fit::same : fit::other_geometry;
    }

    // Readers still attached learn of the retire from their next wake. A name the platform keeps while another holder maps
    // it cannot be created afresh, so the pair is incompatible until that holder leaves.
    template<typename Registry, typename Entry>
    static acquire_result recreate(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity, std::uint64_t found)
    {
        if(!ring_lifetime::stamp_retired(e, found))
            return fail(r, acquire_failure{acquire_bound::transient_join});
        wake(r, e, ctrl_name);
        ring_lifetime::unlink(e);
        return remint(r, e, ctrl_name, geom, publisher, capacity, acquire_bound::incompatible);
    }

    // A retired ring whose holder died before removing its names is removed once the name is proven to map it, and a ring
    // is minted afresh in its place. A name the platform still keeps is a holder on its way out.
    template<typename Registry, typename Entry>
    static acquire_result reclaim_retired(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity)
    {
        if(!ring_lifetime::remove_retired(r.m_broker, e, ctrl_name))
            return fail(r, acquire_failure{acquire_bound::transient_join});
        return remint(r, e, ctrl_name, geom, publisher, capacity, acquire_bound::transient_join);
    }

    template<typename Registry, typename Entry>
    static acquire_result remint(Registry &r, Entry &e, const std::string &ctrl_name, const ring_geometry &geom, const node_id &publisher, std::uint64_t capacity, acquire_bound kept)
    {
        ring_lifetime::drop_creator(e);
        e.slab                 = typename Entry::handle_type{};
        e.control              = typename Entry::handle_type{};
        const region_status cs = r.m_broker.create(ctrl_name, control_region_bytes(geom.cell_count), create_options{}, e.control);
        if(cs == region_status::ok)
            return mint(r, e, ctrl_name, geom, publisher, capacity);
        return fail(r, acquire_failure{cs == region_status::already_exists ? kept : acquire_bound::os_allocator, 0, 0, cs});
    }

    template<typename Registry, typename Entry>
    static void wake(Registry &r, Entry &e, const std::string &ctrl_name)
    {
        std::optional<typename Entry::notifier_type> waker;
        r.m_bind_notifier(waker, *ring_names::header_of(e.control.bytes()), ctrl_name);
        if(waker)
            waker->signal();
    }
};

}

#endif
