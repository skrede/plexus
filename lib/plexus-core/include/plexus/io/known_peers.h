#ifndef HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H
#define HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include <map>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

// The locked discovery-awareness TTL: an entry not re-announced (or refreshed by a heartbeat)
// within this window is aged out of awareness. The value is the smallest TTL that survives
// several missed announce periods at the conservative ~1-3s announce cadence — set by the
// recorded (announce_period, TTL) virtual-clock sweep, not guessed.
constexpr std::uint64_t default_discovery_ttl_ns = 15'000'000'000ull;

// The default (PC) awareness backend: an unbounded heap-backed std::map. The constrained-target
// build swaps in fixed_peer_storage<N> via the Storage template param. Each record carries a
// clock-free last_refreshed tick the engine stamps; nothing reads it on the static_discovery PC
// path (expire_older_than/refresh stay uncalled), so the timestamp is inert there.
class std_map_peer_storage
{
public:
    void put(const node_id &id, const endpoint &ep)
    {
        put(id, ep, 0);
    }
    void put(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        m_table[id] = record{ep, now};
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
        return it->second.ep;
    }
    bool has(const node_id &id) const
    {
        return m_table.find(id) != m_table.end();
    }
    void remove(const node_id &id)
    {
        m_table.erase(id);
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
            fn(id, rec.ep);
    }

private:
    struct record
    {
        endpoint ep;
        std::uint64_t last_refreshed;
    };

    std::map<node_id, record> m_table;
};

// The in-memory awareness table, keyed by node_id (not a discovery name): AWARENESS, not
// connectivity — note_peer records reachability and NEVER dials; the dial is a separate
// demand-driven act the routing_engine owns.
template<typename Storage = std_map_peer_storage>
class basic_known_peers
{
public:
    void note_peer(const node_id &id, const endpoint &ep)
    {
        m_storage.put(id, ep);
    }

    void note_peer(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        m_storage.put(id, ep, now);
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

    void forget(const node_id &id)
    {
        m_storage.remove(id);
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
