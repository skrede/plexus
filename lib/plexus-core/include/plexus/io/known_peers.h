#ifndef HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H
#define HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H

#include "plexus/io/endpoint.h"
#include "plexus/node_id.h"

#include <map>
#include <optional>

namespace plexus::io {

// The in-memory awareness table: a node_id -> endpoint map fed by the discovery
// seam (a stub today). It is AWARENESS, not connectivity — note_peer records that
// a peer with this identity is reachable at this endpoint and NEVER dials; the
// dial is a separate, demand-driven act the routing_engine owns. There is no wire
// here (the table is process-local) and deliberately no enumeration/observer API:
// the only readers are the engine's reach/note_peer paths, which look an identity
// up by its exact node_id. Keyed by node_id, not by a discovery name string — the
// name is not the identity, and a wrong-endpoint dial is caught at the handshake.
//
// A std::map suffices: node_id is a std::array<std::byte,16> with a defaulted
// operator<=>, so it orders out-of-the-box with no std::hash. lookup surfaces
// absence as a std::optional (never a raw pointer into the table).
class known_peers
{
public:
    // Insert or overwrite the awareness entry. NEVER dials — awareness only.
    void note_peer(const node_id &id, const endpoint &ep) { m_table[id] = ep; }

    // The endpoint this identity is reachable at, or absence if unknown.
    std::optional<endpoint> lookup(const node_id &id) const
    {
        auto it = m_table.find(id);
        if(it == m_table.end())
            return std::nullopt;
        return it->second;
    }

    bool contains(const node_id &id) const { return m_table.find(id) != m_table.end(); }

    // Drop an awareness entry (e.g. a peer that left). No wire, no dial side effect.
    void forget(const node_id &id) { m_table.erase(id); }

private:
    std::map<node_id, endpoint> m_table;
};

}

#endif
