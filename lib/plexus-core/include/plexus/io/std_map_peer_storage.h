#ifndef HPP_GUARD_PLEXUS_IO_STD_MAP_PEER_STORAGE_H
#define HPP_GUARD_PLEXUS_IO_STD_MAP_PEER_STORAGE_H

#include "plexus/io/endpoint.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include "plexus/io/detail/route_admission.h"

#include "plexus/node_id.h"

#include <map>
#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io {

// The default (PC) awareness backend: an unbounded heap-backed std::map. A bounded node profile
// substitutes fixed_peer_storage<N> through the Storage template param instead. Each record holds a
// bounded, inline candidate list with direct-first partitioning mirroring the fixed twin; only a
// first admission of a new identity allocates (the map node). The clock-free last_refreshed tick the
// engine stamps is inert on the static_discovery PC path (expire_older_than/refresh stay uncalled).
class std_map_peer_storage
{
public:
    bool put(const node_id &id, const endpoint &ep)
    {
        return put(id, ep, 0);
    }
    // A same-(id, endpoint) re-store refreshes the tick but reports no change: the idempotent
    // re-announce must not bump the graph observer's generation. A fresh direct endpoint overwrites
    // the single direct row in place.
    bool put(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        return admit(id, direct_candidate(ep), now, route_options{});
    }
    bool admit(const node_id &id, const route_candidate &cand, std::uint64_t now, const route_options &opts)
    {
        record &rec        = m_table[id];
        rec.last_refreshed = now;
        const auto r       = detail::admit_candidate(rec.candidates.data(), rec.count, candidate_cap, cand, now, opts);
        if(r.dropped)
            ++m_dropped;
        return r.changed;
    }
    detail::report_admit note_reported(const node_id &id, const route_candidate &cand, std::uint16_t seq, std::uint64_t now, const route_options &opts)
    {
        record &rec  = m_table[id];
        const auto r = detail::note_reported_row(rec.candidates.data(), rec.count, candidate_cap, rec.last_refreshed, cand, seq, now, opts);
        m_dropped += static_cast<std::size_t>(r == detail::report_admit::dropped);
        return r;
    }
    bool remove_transitive(const node_id &id, const node_id &via)
    {
        auto it = m_table.find(id);
        if(it == m_table.end() || !detail::remove_transitive_row(it->second.candidates.data(), it->second.count, via))
            return false;
        if(it->second.count == 0)
            m_table.erase(it);
        return true;
    }
    bool mark_unreachable_via(const node_id &id, const node_id &via)
    {
        auto it = m_table.find(id);
        return it != m_table.end() && detail::mark_reachability_via(it->second.candidates.data(), it->second.count, via, graph::reachability::unreachable) != 0;
    }
    bool mark_reachable_via(const node_id &id, const node_id &via)
    {
        auto it = m_table.find(id);
        return it != m_table.end() && detail::mark_reachability_via(it->second.candidates.data(), it->second.count, via, graph::reachability::reachable) != 0;
    }
    std::size_t reset_reported_windows(const node_id &via)
    {
        std::size_t n = 0;
        for(auto &[id, rec] : m_table)
            n += detail::reset_windows_via(rec.candidates.data(), rec.count, via);
        return n;
    }
    bool withdraw_seq_fresh(const node_id &id, const node_id &via, std::uint16_t seq)
    {
        auto it = m_table.find(id);
        return it != m_table.end() && detail::admit_via_seq(it->second.candidates.data(), it->second.count, via, seq);
    }
    template<typename Fn>
    void for_each_origin_via(const node_id &via, Fn fn) const
    {
        for(const auto &[id, rec] : m_table)
            for(std::size_t i = 0; i < rec.count; ++i)
                if(!rec.candidates[i].is_direct() && rec.candidates[i].reach.via == via)
                {
                    fn(id);
                    break;
                }
    }
    void refresh(const node_id &id, std::uint64_t now)
    {
        if(auto it = m_table.find(id); it != m_table.end())
            it->second.last_refreshed = now;
    }
    std::optional<endpoint> get(const node_id &id) const
    {
        auto it = m_table.find(id);
        if(it == m_table.end())
            return std::nullopt;
        return detail::direct_endpoint(it->second.candidates.data(), it->second.count);
    }
    bool has(const node_id &id) const
    {
        return m_table.find(id) != m_table.end();
    }
    std::span<const route_candidate> candidates(const node_id &id) const
    {
        auto it = m_table.find(id);
        if(it == m_table.end())
            return {};
        return {it->second.candidates.data(), it->second.count};
    }
    bool remove(const node_id &id)
    {
        return m_table.erase(id) != 0;
    }
    std::size_t dropped() const noexcept
    {
        return m_dropped;
    }
    template<typename Report>
    void expire_older_than(std::uint64_t deadline, Report report)
    {
        for(auto it = m_table.begin(); it != m_table.end();)
        {
            if(it->second.last_refreshed < deadline)
            {
                report(it->first);
                it = m_table.erase(it);
            }
            else
                ++it;
        }
    }
    template<typename Fn>
    void for_each(Fn fn) const
    {
        for(const auto &[id, rec] : m_table)
            if(auto ep = detail::direct_endpoint(rec.candidates.data(), rec.count))
                fn(id, *ep);
    }

    // Every candidate row of every identity, direct and via-only alike, each with its own reach and
    // provenance — the extended-world enumeration a reported peer surfaces through (for_each yields the
    // direct row only). A direct-only identity yields exactly its one direct row, so the direct-only
    // enumeration stays byte-identical.
    template<typename Fn>
    void for_each_candidate(Fn fn) const
    {
        for(const auto &[id, rec] : m_table)
            for(std::size_t i = 0; i < rec.count; ++i)
                fn(id, rec.candidates[i].reach, rec.candidates[i].origin);
    }

private:
    static constexpr std::size_t candidate_cap = 4;

    struct record
    {
        std::array<route_candidate, candidate_cap> candidates{};
        std::size_t count{0};
        std::uint64_t last_refreshed{0};
    };

    std::map<node_id, record> m_table;
    std::size_t m_dropped{0};
};

}

#endif
