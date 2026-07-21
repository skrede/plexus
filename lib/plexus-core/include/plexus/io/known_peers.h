#ifndef HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H
#define HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/detail/route_admission.h"

#include <map>
#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

// The locked discovery-awareness TTL: an entry not re-announced (or refreshed by a heartbeat)
// within this window is aged out of awareness. The value is the smallest TTL that survives
// several missed announce periods at the conservative ~1-3s announce cadence — set by the
// recorded (announce_period, TTL) virtual-clock sweep, not guessed.
constexpr std::uint64_t default_discovery_ttl_ns = 15'000'000'000ull;

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

// The in-memory awareness table, keyed by node_id (not a discovery name): AWARENESS, not
// connectivity — note_peer records reachability and NEVER dials; the dial is a separate
// demand-driven act the routing_engine owns.
template<typename Storage = std_map_peer_storage>
class basic_known_peers
{
public:
    bool note_peer(const node_id &id, const endpoint &ep)
    {
        return m_storage.put(id, ep);
    }

    bool note_peer(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        return m_storage.put(id, ep, now);
    }

    // Extends an EXISTING entry's freshness; a no-op for an unknown id (a heartbeat from a
    // non-discovered peer must not invent awareness).
    void refresh(const node_id &id, std::uint64_t now)
    {
        m_storage.refresh(id, now);
    }

    std::optional<endpoint> lookup(const node_id &id) const
    {
        return m_storage.get(id);
    }

    bool contains(const node_id &id) const
    {
        return m_storage.has(id);
    }

    bool forget(const node_id &id)
    {
        return m_storage.remove(id);
    }

    template<typename Report>
    void expire_older_than(std::uint64_t deadline, Report report)
    {
        m_storage.expire_older_than(deadline, std::move(report));
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        m_storage.for_each(std::move(fn));
    }

private:
    Storage m_storage;
};

using known_peers = basic_known_peers<>;

}

#endif
