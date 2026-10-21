#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_CLOSE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_CLOSE_H

#include "plexus/io/endpoint.h"
#include "plexus/io/endpoint_id.h"

#include "plexus/io/detail/pending_ops.h"

#include "plexus/node_id.h"

namespace plexus::io::detail {

// The sequence is itself one operation, so nothing it completes synchronously settles the engine before it ends, and a
// completion that closes again finds the engine closing. With its callbacks detached first, whatever the closing
// transport still completes or fails reaches no slot: the transport retires it. The liveliness tick, the pending calls
// and each slot count as one operation of the engine until they hold nothing.
template<typename Engine>
void close_engine(Engine &e)
{
    if(e.m_ops.closing())
        return;
    e.m_ops.begin();
    e.m_ops.close();
    e.m_transport.on_accepted(nullptr);
    e.m_transport.on_dialed(nullptr);
    e.m_transport.on_dial_failed(nullptr);
    hold_until_settled(e.m_ops, e.m_monitor.pending());
    e.m_monitor.shutdown();
    hold_until_settled(e.m_ops, e.m_procedures.pending());
    e.m_procedures.close();
    e.m_retry_timer.cancel();
    e.m_registry.close(e.m_ops);
    e.m_transport.close();
    e.m_ops.end();
}

// Only a slot dial() made answers, so an endpoint reached through discovery or never dialed is left alone.
template<typename Engine>
void hang_up_endpoint(Engine &e, const endpoint &ep)
{
    const node_id id = endpoint_id(ep);
    if(e.m_registry.id_for_endpoint(ep) == id)
        e.m_registry.hang_up(id);
}

}

#endif
