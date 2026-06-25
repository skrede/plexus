#ifndef HPP_GUARD_PLEXUS_IO_HANDSHAKE_PROTOCOL_H
#define HPP_GUARD_PLEXUS_IO_HANDSHAKE_PROTOCOL_H

#include "plexus/io/host_fingerprint.h"

#include "plexus/io/security/attach_policy.h"

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

enum class peer_fsm_state : std::uint8_t
{
    not_connected,
    dialing,
    handshaking,
    handshake_resolved
};

enum class fsm_action : std::uint8_t
{
    none,
    send_request,
    send_response,
    complete,
    retry,
    abort
};

enum class handshake_outcome : std::uint8_t
{
    none,
    accept_outbound,
    accept_inbound,
    reject_version,
    reject_identity,
    reject_unauthorized
};

enum class dedup_decision : std::uint8_t
{
    none,
    keep_outbound,
    keep_inbound
};

struct fsm_step_result
{
    fsm_action action{fsm_action::none};
    handshake_outcome outcome{handshake_outcome::none};
    dedup_decision dedup{dedup_decision::none};
};

struct handshake_fsm_config
{
    node_id self_id;
    std::uint8_t version_major;
    std::uint8_t version_minor;
    std::uint8_t compatible_version_major;
    std::uint8_t compatible_version_minor;
    host_fingerprint local_fingerprint{};
    const security::attach_policy *attach_policy{nullptr};
    // 0 (the default) aborts on the first handshaking-state timeout exactly as before; a non-zero
    // budget re-sends the request that many times before surrendering to the same abort. Only a
    // directly-dialed point-to-point slot sets it, so the generic timeout edge is unchanged by default.
    std::uint32_t handshake_retry{0};
};

}

#endif
