#ifndef HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H
#define HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include <map>
#include <optional>

namespace plexus::io {

// The default (PC) awareness backend: an unbounded heap-backed std::map. The constrained-target
// build swaps in fixed_peer_storage<N> via the Storage template param.
class std_map_peer_storage
{
public:
    void put(const node_id &id, const endpoint &ep)
    {
        m_table[id] = ep;
    }
    std::optional<endpoint> get(const node_id &id) const
    {
        auto it = m_table.find(id);
        if(it == m_table.end())
            return std::nullopt;
        return it->second;
    }
    bool has(const node_id &id) const
    {
        return m_table.find(id) != m_table.end();
    }
    void remove(const node_id &id)
    {
        m_table.erase(id);
    }

private:
    std::map<node_id, endpoint> m_table;
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

private:
    Storage m_storage;
};

using known_peers = basic_known_peers<>;

}

#endif
