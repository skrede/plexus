#ifndef HPP_GUARD_PLEXUS_IO_LIFECYCLE_EVENT_H
#define HPP_GUARD_PLEXUS_IO_LIFECYCLE_EVENT_H

#include "plexus/node_id.h"

#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_fsm.h"

#include <string>

namespace plexus::io {

enum class lifecycle_edge : std::uint8_t
{
    connected,
    disconnected,
    reconnected,
    dead,
    ready,
    rejected
};

// node_name is OWNED (not a view into session state): the engine delivers every edge
// on a POSTED turn that outlives the fire-site's stack frame.
struct lifecycle_event
{
    lifecycle_edge edge;
    node_id id;
    std::string node_name;
    peer_kind kind;
    handshake_outcome reason; // meaningful only on the rejected edge
};

}

#endif
