#ifndef HPP_GUARD_PLEXUS_IO_OBSERVER_H
#define HPP_GUARD_PLEXUS_IO_OBSERVER_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/message_info.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/security_event.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/recording/wire_record.h"

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

    virtual void on_peer_connected(const node_id &, std::string_view, peer_kind)
    {
    }
    virtual void on_peer_disconnected(const node_id &, std::string_view, peer_kind)
    {
    }
    virtual void on_peer_reconnected(const node_id &, std::string_view, peer_kind)
    {
    }
    virtual void on_peer_dead(const node_id &, std::string_view, peer_kind)
    {
    }
    virtual void on_peer_ready(const node_id &, std::string_view, peer_kind)
    {
    }
    virtual void on_peer_rejected(const node_id &, std::string_view, handshake_outcome)
    {
    }
    virtual void on_drop(const io::detail::drop_event &)
    {
    }
    virtual void on_security(const security_event &)
    {
    }

    // The data-path taps, posted on the executor like the lifecycle edges. A publish
    // fires on_message_published once at the fan-out gate; on_message_delivered fires
    // once per destination, carrying the borrowed view (its owner shares the delivered
    // buffer — no copy). The rpc taps surface the borrowed call/serve/reply views, and
    // on_qos_change reports a subscriber attach's resolved verdict. All default-empty.
    virtual void on_message_published(std::string_view, const message_view &)
    {
    }
    virtual void on_message_delivered(std::string_view, const message_info &, const message_view &)
    {
    }
    virtual void on_rpc_call(std::string_view, const rpc_view &)
    {
    }
    virtual void on_rpc_serve(std::string_view, const rpc_view &)
    {
    }
    virtual void on_rpc_reply(std::string_view, const rpc_reply_view &)
    {
    }
    virtual void on_qos_change(const qos_change_event &)
    {
    }

    // The declaration-lifecycle edges: a node's own create/destroy and a topic endpoint's
    // declare/drop/register/retire. They fire once per node or endpoint, not per message —
    // cold edges, so they are NOT gated by observes_data_path and post regardless of the
    // data-tap opt-in. fqn rides as a borrowed view valid for the call.
    virtual void on_participant(const participant_event &)
    {
    }
    virtual void on_endpoint(std::string_view, const endpoint_event &)
    {
    }

    // The wire-fidelity tap: one captured framed packet, posted on the executor. Unlike the
    // message edges it is NOT gated by observes_data_path — it gates STRUCTURALLY: a node
    // that did not compose the recording_channel decorator for a transport never produces a
    // captured frame, so the edge simply never fires (no per-frame runtime opt-in branch).
    // The carried wire_record's bytes are owned for the posted turn (the engine pins them
    // across the post). Default-empty like every other cold edge.
    virtual void on_wire(const recording::wire_record &)
    {
    }

    // The data-path opt-in: the message/rpc/qos taps fire once per publish/destination/call,
    // so their posted fan-out is a HOT cost (an fqn copy + a posted closure per emit). The
    // engine fans them only while at least one registered observer declares interest here, so
    // a node's always-on lifecycle machinery (and any observer that watches only connection
    // edges) pays nothing on the data path. Override to true to receive the data-path taps;
    // the default keeps the hot path one predictable branch for a lifecycle-only observer.
    virtual bool observes_data_path() const
    {
        return false;
    }
};

}

#endif
