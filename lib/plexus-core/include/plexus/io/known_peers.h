#ifndef HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H
#define HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H

#include "plexus/io/endpoint.h"
#include "plexus/node_id.h"

#include <map>
#include <optional>

namespace plexus::io {

// The default awareness backend: a std::map<node_id, endpoint>. node_id is a
// std::array<std::byte,16> with a defaulted operator<=>, so it orders out-of-the-box
// with no std::hash. This is the PC storage policy — unbounded, heap-backed; the
// constrained-target build swaps in fixed_peer_storage<N> via the Storage template param.
// It exposes ONLY the four operations the awareness verbs call (put/get/has/remove) and
// no enumeration, so basic_known_peers can leak no map-specific surface.
class std_map_peer_storage
{
public:
    void                    put(const node_id &id, const endpoint &ep) { m_table[id] = ep; }
    std::optional<endpoint> get(const node_id &id) const
    {
        auto it = m_table.find(id);
        if(it == m_table.end())
            return std::nullopt;
        return it->second;
    }
    bool has(const node_id &id) const { return m_table.find(id) != m_table.end(); }
    void remove(const node_id &id) { m_table.erase(id); }

private:
    std::map<node_id, endpoint> m_table;
};

// The in-memory awareness table: a node_id -> endpoint map fed by the discovery
// seam (a stub today). It is AWARENESS, not connectivity — note_peer records that
// a peer with this identity is reachable at this endpoint and NEVER dials; the
// dial is a separate, demand-driven act the routing_engine owns. There is no wire
// here (the table is process-local) and deliberately no enumeration/observer API:
// the only readers are the engine's reach/note_peer paths, which look an identity
// up by its exact node_id. Keyed by node_id, not by a discovery name string — the
// name is not the identity, and a wrong-endpoint dial is caught at the handshake.
//
// The backing store is a template parameter: std::map by default (the PC policy),
// or a dep-free fixed-capacity flat array on a constrained target. The four verbs
// delegate to the Storage; nothing beyond them is exposed, so the storage choice
// never leaks through this surface. lookup surfaces absence as a std::optional
// (never a raw pointer into the table).
template<typename Storage = std_map_peer_storage>
class basic_known_peers
{
public:
    // Insert or overwrite the awareness entry. NEVER dials — awareness only.
    void note_peer(const node_id &id, const endpoint &ep) { m_storage.put(id, ep); }

    // The endpoint this identity is reachable at, or absence if unknown.
    std::optional<endpoint> lookup(const node_id &id) const { return m_storage.get(id); }

    bool contains(const node_id &id) const { return m_storage.has(id); }

    // Drop an awareness entry (e.g. a peer that left). No wire, no dial side effect.
    void forget(const node_id &id) { m_storage.remove(id); }

private:
    Storage m_storage;
};

// The PC type name stays byte-stable: io::known_peers is the std::map-backed default,
// so the routing_engine accessor signature and the PC codegen are unchanged.
using known_peers = basic_known_peers<>;

}

#endif
