#ifndef HPP_GUARD_PLEXUS_IO_OBSERVER_H
#define HPP_GUARD_PLEXUS_IO_OBSERVER_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/security_event.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/node_id.h"

#include <string_view>

namespace plexus::io {

// The single public observer surface user code subclasses to learn a node's session
// events: the six peer-liveness lifecycle edges, a shed-frame drop, and a security
// transition. It is a cold-path, runtime-injected virtual interface registered on the
// routing engine, which fans every edge out POSTED on the Policy executor over a
// snapshot — NEVER synchronously from the fire-site. That posted indirection is the DoS
// guard: an untrusted UDP flood drives the receive-side drop sites, and firing an edge
// inline per packet would amplify the flood into a synchronous-callback DoS. Each edge
// defaults to an empty body so an observer overrides only the transitions it cares about,
// and an observer that only wants the always-on counters pays for no override.
//
// For the lifecycle edges the node_id is the peer's stable identity, name a borrowed view
// valid for the call, and peer_kind tells dialed from accepted (an accepted peer never
// fires reconnect or dead). on_peer_rejected carries the FSM's refusal cause instead of a
// kind, since a rejected handshake never establishes a peer to classify. The drop_event is
// a by-value POD (scalars + node_id), so the posted turn carries a coalesced copy with no
// borrowed lifetime; the security_event is the distinct security-surface fan-out (an
// unauthorized attach ALSO rides the lifecycle rejected edge).
class observer
{
public:
    virtual ~observer() = default;

    virtual void on_peer_connected(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_disconnected(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_reconnected(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_dead(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_ready(const node_id &, std::string_view, peer_kind) {}
    virtual void on_peer_rejected(const node_id &, std::string_view, handshake_outcome) {}
    virtual void on_drop(const io::detail::drop_event &) {}
    virtual void on_security(const security_event &) {}
};

// The shared inert observer a build context defaults to when no observer is installed:
// every edge is the base no-op, so an unobserved node pays one predictable branch. A
// function-local static bound by reference (no namespace-scope singleton), mirroring
// shared_null_logger.
inline observer &shared_null_observer()
{
    static observer sink;
    return sink;
}

}

#endif
