#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_CLOSE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_CLOSE_H

#include "plexus/io/detail/pending_ops.h"

namespace plexus::io::detail {

// With its callbacks detached first, whatever the closing transport still completes or fails reaches no slot: the
// transport retires it. Each slot counts as one operation of the engine until it holds nothing.
template<typename Engine>
void close_engine(Engine &e)
{
    if(e.m_ops.closing())
        return;
    e.m_transport.on_accepted(nullptr);
    e.m_transport.on_dialed(nullptr);
    e.m_transport.on_dial_failed(nullptr);
    e.m_registry.close(e.m_ops);
    e.m_transport.close();
    e.m_ops.close();
}

}

#endif
