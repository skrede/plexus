#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SESSION_SCAN_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SESSION_SCAN_H

#include "plexus/node_id.h"

namespace plexus::io::detail {

// A peer that both dials and accepts holds two sessions under two slot keys but one proven
// identity, and the table is keyed by that identity.
template<typename Engine>
bool any_session_for(Engine &e, const node_id &proven)
{
    bool found = false;
    e.m_registry.for_each_connected([&](const node_id &, typename Engine::session_type &s) { found = found || s.peer_identity() == proven; });
    return found;
}

// A live session for a PROVEN identity — an accepted session's slot key is provisional, so the table
// is scanned by handshake-proven identity, mirroring relay_broadcast's loop guard.
template<typename Engine>
typename Engine::session_type *session_by_identity(Engine &e, const node_id &proven)
{
    typename Engine::session_type *found = nullptr;
    e.m_registry.for_each_connected(
            [&](const node_id &, typename Engine::session_type &s)
            {
                if(s.peer_identity() == proven)
                    found = &s;
            });
    return found;
}

}

#endif
