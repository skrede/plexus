#ifndef HPP_GUARD_PLEXUS_IO_LIVENESS_STORAGE_H
#define HPP_GUARD_PLEXUS_IO_LIVENESS_STORAGE_H

#include "plexus/io/detail/fixed_flat_map.h"
#include "plexus/io/detail/liveliness_scan.h"
#include "plexus/io/detail/endpoint_liveness.h"

#include "plexus/node_id.h"

#include <map>
#include <cstddef>
#include <cstdint>

namespace plexus::io {

// The default (PC) monitor backend: two unbounded heap-backed std::maps — the exact
// deadline (per node_id + topic) and lease (per node_id) tables the monitor held inline
// before the storage swap. A constrained-target build swaps in fixed_liveness_storage
// via the monitor's Storage template param. The upsert verbs default-initialize the
// endpoint_liveness axis the way the monitor's register did (period/lease set, stamp
// refreshed, latch cleared); a stamp mutates through find_* with no table growth.
class std_map_liveness_storage
{
public:
    void upsert_deadline(const detail::deadline_key &key, std::uint64_t period, std::uint64_t now)
    {
        detail::endpoint_liveness &d = m_deadlines[key];
        d.deadline_period_ns         = period;
        d.last_data_seen_ns          = now;
        d.deadline_violated          = false;
    }

    void upsert_lease(const node_id &id, std::uint64_t lease_ns, std::uint64_t now)
    {
        detail::endpoint_liveness &l = m_leases[id];
        l.lease_ns                   = lease_ns;
        l.last_seen_ns               = now;
        l.lease_expired              = false;
    }

    void erase_endpoint(const node_id &id)
    {
        for(auto it = m_deadlines.begin(); it != m_deadlines.end();)
            it = (it->first.first == id) ? m_deadlines.erase(it) : std::next(it);
        m_leases.erase(id);
    }

    detail::endpoint_liveness *find_deadline(const detail::deadline_key &key)
    {
        auto it = m_deadlines.find(key);
        return it == m_deadlines.end() ? nullptr : &it->second;
    }

    detail::endpoint_liveness *find_lease(const node_id &id)
    {
        auto it = m_leases.find(id);
        return it == m_leases.end() ? nullptr : &it->second;
    }

    template<typename Fn>
    void for_each_deadline(Fn fn)
    {
        for(auto &[key, state] : m_deadlines)
            fn(key, state);
    }

    template<typename Fn>
    void for_each_lease(Fn fn)
    {
        for(auto &[id, state] : m_leases)
            fn(id, state);
    }

private:
    std::map<detail::deadline_key, detail::endpoint_liveness> m_deadlines;
    std::map<node_id, detail::endpoint_liveness> m_leases;
};

// The constrained-target (MCU) monitor backend: the same verb surface over two
// fixed_flat_maps. Two capacities because the deadline table is peers × subscribed
// topics while the lease table is peers. The (N+1)-th distinct key of either table
// fails closed — the defined refusal fixed_flat_map establishes.
template<std::size_t NLeases, std::size_t NDeadlines>
class fixed_liveness_storage
{
public:
    void upsert_deadline(const detail::deadline_key &key, std::uint64_t period, std::uint64_t now)
    {
        detail::endpoint_liveness &d = m_deadlines.at_or_insert(key);
        d.deadline_period_ns         = period;
        d.last_data_seen_ns          = now;
        d.deadline_violated          = false;
    }

    void upsert_lease(const node_id &id, std::uint64_t lease_ns, std::uint64_t now)
    {
        detail::endpoint_liveness &l = m_leases.at_or_insert(id);
        l.lease_ns                   = lease_ns;
        l.last_seen_ns               = now;
        l.lease_expired              = false;
    }

    void erase_endpoint(const node_id &id)
    {
        m_deadlines.erase_if([&id](const detail::deadline_key &key) { return key.first == id; });
        m_leases.erase(id);
    }

    detail::endpoint_liveness *find_deadline(const detail::deadline_key &key)
    {
        return m_deadlines.find(key);
    }

    detail::endpoint_liveness *find_lease(const node_id &id)
    {
        return m_leases.find(id);
    }

    template<typename Fn>
    void for_each_deadline(Fn fn)
    {
        m_deadlines.for_each(fn);
    }

    template<typename Fn>
    void for_each_lease(Fn fn)
    {
        m_leases.for_each(fn);
    }

private:
    detail::fixed_flat_map<detail::deadline_key, detail::endpoint_liveness, NDeadlines> m_deadlines;
    detail::fixed_flat_map<node_id, detail::endpoint_liveness, NLeases> m_leases;
};

}

#endif
