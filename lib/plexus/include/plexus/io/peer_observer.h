#ifndef HPP_GUARD_PLEXUS_IO_PEER_OBSERVER_H
#define HPP_GUARD_PLEXUS_IO_PEER_OBSERVER_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include <string_view>

namespace plexus::io {

// The public observer surface user code subclasses to learn a node's peer
// liveness. It is a cold-path, runtime-injected virtual interface (the
// architecture's cold-path discipline) registered on the routing engine, which
// fans every edge out POSTED on the Policy executor — never synchronously from the
// fire-site. Each edge defaults to an empty body so an observer overrides only the
// transitions it cares about; the node_id is the peer's stable identity, name a
// borrowed view valid for the call, and peer_kind tells dialed from accepted (an
// accepted peer never fires reconnect or dead). on_peer_rejected carries the FSM's
// refusal cause instead of a kind, since a rejected handshake never establishes a
// peer to classify.
class peer_observer
{
public:
    virtual ~peer_observer() = default;

    virtual void on_peer_connected(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_disconnected(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_reconnected(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_dead(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_ready(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_rejected(const node_id &, std::string_view, handshake_outcome) {}
};

}

#endif
