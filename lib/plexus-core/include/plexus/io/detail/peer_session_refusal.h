#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REFUSAL_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REFUSAL_H

#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/wire/close_cause.h"

namespace plexus::io::detail {

template<typename Session>
void refuse_posture(Session &s)
{
    s.m_negotiator.clear_identity();
    fire_lifecycle(s, lifecycle_edge::rejected, handshake_outcome::reject_unauthorized);
    fire_security(s, security_kind::posture_mismatch);
    s.close_for_protocol_error(wire::close_cause::invalid_magic);
}

// A failed local RNG or MAC is refused before anything derived from it is sent or completes the
// session, and its event names the local side so the refusal is never blamed on the peer.
template<typename Session>
void refuse_degraded(Session &s)
{
    s.m_negotiator.clear_identity();
    fire_lifecycle(s, lifecycle_edge::rejected, handshake_outcome::reject_unauthorized);
    fire_security(s, security_kind::local_crypto_failure);
    s.close_for_protocol_error(wire::close_cause::invalid_magic);
}

}

#endif
