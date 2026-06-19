#ifndef HPP_GUARD_PLEXUS_IO_HANDSHAKE_PROTOCOL_H
#define HPP_GUARD_PLEXUS_IO_HANDSHAKE_PROTOCOL_H

#include "plexus/io/security/attach_policy.h"
#include "plexus/io/shm/same_host.h"

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

// State the FSM advances through during a handshake. There is no `connected`
// enumerator: `connected` is bridge-owned, not an FSM state. handshake_resolved
// is the terminal state — the precondition the bridge composes onto.
enum class peer_fsm_state : std::uint8_t
{
    not_connected,
    dialing,
    handshaking,
    handshake_resolved
};

// Action the bridge must perform in response to an FSM step. The FSM is pure: it
// touches no socket, posts to no strand, schedules no timer, and moves no bytes.
enum class fsm_action : std::uint8_t
{
    none,
    send_request,
    send_response,
    complete,
    retry,
    abort
};

// Outcome of a handshake step. reject_identity is the equal-node_id collision;
// reject_version covers both the exact-protocol gate and the compat-window failure.
enum class handshake_outcome : std::uint8_t
{
    none,
    accept_outbound,
    accept_inbound,
    reject_version,
    reject_identity,
    reject_unauthorized
};

// Dedup arbitration verdict, populated only when a step completes with a pending
// counter-direction connection. The bridge owns the channel handles and drops the
// loser's; the FSM returns only the verdict.
enum class dedup_decision : std::uint8_t
{
    none,
    keep_outbound,
    keep_inbound
};

// Step result: the FSM's pure-data channel back to the bridge.
struct fsm_step_result
{
    fsm_action        action{fsm_action::none};
    handshake_outcome outcome{handshake_outcome::none};
    dedup_decision    dedup{dedup_decision::none};
};

// Static configuration for one handshake. node_id and the four self-version fields
// are required with NO default — a zeroed default would silently ship a real,
// comparable identity, so the categories stay distinct (required != default).
//
// local_fingerprint is required-WITH-default: a null (zero) fingerprint is the
// meaningful "no co-location signal advertised" value. attach_policy is the
// node-level admission gate, borrowed; a null policy is accept-any.
struct handshake_fsm_config
{
    node_id                        self_id;
    std::uint8_t                   version_major;
    std::uint8_t                   version_minor;
    std::uint8_t                   compatible_version_major;
    std::uint8_t                   compatible_version_minor;
    shm::host_fingerprint          local_fingerprint{};
    const security::attach_policy *attach_policy{nullptr};
};

}

#endif
