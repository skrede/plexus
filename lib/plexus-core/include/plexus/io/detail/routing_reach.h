#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_REACH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_REACH_H

#include "plexus/io/endpoint.h"
#include "plexus/io/node_name.h"
#include "plexus/io/endpoint_id.h"

#include "plexus/io/detail/routing_session_scan.h"

#include "plexus/node_id.h"

namespace plexus::io::detail {

// A session not yet torn down owns the slot, so starting the driver would tear it down. A driver that will not redial
// yields the slot to a fresh start; a forget reaches this only mid-handshake, since both callers stop first on a
// complete session.
template<typename Engine>
bool has_live_session(Engine &e, const node_id &id)
{
    auto *s       = e.registry().session_for(id);
    const auto &d = e.registry().driver_for(id);
    return s != nullptr && !s->pending().closing() && !d.redial_denied() && !d.is_surrendered();
}

template<typename Engine>
void reach_peer(Engine &e, const node_id &id)
{
    if(e.m_registry.is_connected(id) || detail::any_session_for(e, id))
        return;
    auto ep = e.m_known_peers.dial_target(id);
    if(!ep)
        return;
    if(!e.m_registry.ensure_slot(id, *ep, node_name_of(id)) || detail::has_live_session(e, id))
        return;
    e.m_registry.driver_for(id).start();
}

template<typename Engine>
void dial_endpoint(Engine &e, const endpoint &ep)
{
    const auto id = endpoint_id(ep);
    if(e.m_registry.is_connected(id))
        return;
    if(!e.m_registry.ensure_slot(id, ep, node_name_of(id)) || detail::has_live_session(e, id))
        return;
    e.m_registry.driver_for(id).start();
}

}

#endif
