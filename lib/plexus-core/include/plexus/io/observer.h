#ifndef HPP_GUARD_PLEXUS_IO_OBSERVER_H
#define HPP_GUARD_PLEXUS_IO_OBSERVER_H

#include "plexus/node_id.h"

#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_info.h"
#include "plexus/io/security_event.h"
#include "plexus/io/observation_events.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/io/recording/wire_record.h"

#include <string_view>

namespace plexus::io {

// Every edge fans out POSTED on the Policy executor over a snapshot, NEVER synchronously from the
// fire-site: an untrusted UDP flood drives the receive-side drop sites, and an inline per-packet
// edge would amplify the flood into a synchronous-callback DoS.
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

    virtual void on_participant(const participant_event &)
    {
    }
    virtual void on_endpoint(std::string_view, const endpoint_event &)
    {
    }

    // Gated STRUCTURALLY, not by observes_data_path: a node that did not compose the
    // recording_channel decorator never produces a captured frame, so the edge never fires.
    virtual void on_wire(const recording::wire_record &)
    {
    }

    // The data-path opt-in: the engine fans the message/rpc/qos taps only while at least one
    // registered observer declares interest here, so a lifecycle-only observer pays nothing.
    virtual bool observes_data_path() const
    {
        return false;
    }
};

}

#endif
