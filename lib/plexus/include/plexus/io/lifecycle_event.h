#ifndef HPP_GUARD_PLEXUS_IO_LIFECYCLE_EVENT_H
#define HPP_GUARD_PLEXUS_IO_LIFECYCLE_EVENT_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include <string>

namespace plexus::io {

// Which lifecycle transition a carried event represents. The session reports each
// edge as a value the engine fans out to the registered observers: connected and
// disconnected bound a session's lifetime; reconnected replaces connected when a
// dialed peer re-establishes after a drop; dead marks a dialed peer that crossed
// its redial surrender bound; ready marks a connected peer whose pending subscribe
// loop has drained; rejected marks a handshake the FSM refused.
enum class lifecycle_edge : std::uint8_t
{
    connected,
    disconnected,
    reconnected,
    dead,
    ready,
    rejected
};

// The value the session hands UP to the engine fan-out, by value. node_name is an
// OWNED std::string (copied, never a view into session state): the engine delivers
// every edge on a POSTED turn that outlives the fire-site's stack frame, so the
// carrier must own its identity. reason is meaningful only on the rejected edge —
// it carries the FSM's refusal cause; the other edges leave it none.
struct lifecycle_event
{
    lifecycle_edge    edge;
    node_id           id;
    std::string       node_name;
    peer_kind         kind;
    handshake_outcome reason;
};

}

#endif
