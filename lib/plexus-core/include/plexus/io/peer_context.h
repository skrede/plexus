#ifndef HPP_GUARD_PLEXUS_IO_PEER_CONTEXT_H
#define HPP_GUARD_PLEXUS_IO_PEER_CONTEXT_H

#include "plexus/io/endpoint.h"
#include "plexus/io/epoch_source.h"
#include "plexus/io/host_fingerprint.h"
#include "plexus/node_id.h"
#include "plexus/policy.h"

#include <memory>
#include <string>

namespace plexus::io {

// The per-peer incarnation DATA record: the cohesive value bundle one peer_session
// is built from by a single reference. The owner (the harness today, the per-peer
// registry slot tomorrow) holds one of these next to the connection slot; it OWNS
// the live byte_channel (the caller-ownership rule — the session only borrows it),
// carries the peer's stable node_id (the registry key the node_name is derived
// from), the dial endpoint the slot redials, and the epoch_source the session draws
// its session_id from.
//
// The record OUTLIVES every peer_session built from it, so a redialed fresh
// session automatically draws a STRICTLY later epoch from the same source — the
// cross-incarnation monotonicity the staleness gate relies on is structural, not
// stamped in by a caller. There is NO mutator that mints or sets the epoch: the
// only advance is a session drawing epochs.next() on completion.
//
// The redial driver is deliberately NOT a member here. It is not default-
// constructible (it needs a Transport, executor, config, endpoint and seed) and is
// owned OUTSIDE peer_session; it lives as a SIBLING of this record at per-peer-slot
// lifetime (harness-owned in the redial oracles, registry-slot-owned next).
// Keeping it out leaves the record a pure value bundle and the non-redialing slots
// free of dead per-slot scaffolding. So this templates over Policy alone.
template<typename Policy>
    requires plexus::Policy<Policy>
struct peer_context
{
    using channel_type = typename Policy::byte_channel_type;

    std::unique_ptr<channel_type> channel;       // the live connection; the session borrows it
    node_id                       peer_id{};     // the peer's stable identity (the registry key)
    std::string                   node_name;     // the forwarder-peer key, set once at construction
    endpoint                      dial_endpoint; // the endpoint the slot dials/redials
    epoch_source                  epochs;        // the per-peer well each incarnation draws from
    // Set once on the first complete, NEVER cleared — survives teardown so a redial
    // fires reconnected, not connected. It lives HERE (the record outlives every
    // incarnation) and NOT on peer_session: a session-local flag would reset every
    // reconnect (build_into destroys+recreates the session) and mis-fire connected.
    bool has_ever_connected{false};
    // The same-host eligibility verdict for THIS peer, recorded by the session on
    // each handshake completion from the advertised-vs-local fingerprint compare.
    // It is the gate the shared-memory upgrade reads: only a same-host pair
    // ever attempts a ring acquire. A plain value with a fail-closed default (false
    // = not co-located until proven), re-evaluated every completion — its absence is
    // not meaningful, only its value.
    bool same_host{false};
};

}

#endif
