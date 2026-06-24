#ifndef HPP_GUARD_PLEXUS_IO_HOST_IDENTITY_H
#define HPP_GUARD_PLEXUS_IO_HOST_IDENTITY_H

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/cert_facts.h"

#include "plexus/node_id.h"

namespace plexus::io {

// The authenticated peer's host identity, derived from the security step that admitted
// it — NEVER from a self-asserted TXT/wire claim. Two paths converge on the same value
// type: a TLS peer's identity is the SPKI-derived node_id (cert_facts::to_node_id); a
// PSK peer's identity is the node_id carried in the VERIFIED attach_facts (the id that
// produced the validated proof, bound by the challenge-response). The free functions
// below are the pure derivations; the resolved-session accessor returns one of them only
// after the attach resolves (absent before that).

// The PSK-path identity: the responder_id is the accepting side's node_id, the
// initiator_id the dialing side's. The authenticated peer (the OTHER end) is whichever
// the local node is not — the bridge passes the verified facts plus the local role and
// reads the peer's half. This reads ONLY the authenticated binding, never a wire claim.
inline node_id authenticated_peer_id(const security::attach_facts &facts) noexcept
{
    return facts.role == security::attach_role::initiator ? facts.responder_id : facts.initiator_id;
}

// The TLS-path identity: the SPKI-derived node_id already computed for the verified
// peer certificate (re-exported here so the host-identity accessor names one entry
// point for both postures).
inline node_id authenticated_peer_id(const security::cert_facts &facts) noexcept
{
    return security::to_node_id(facts);
}

}

#endif
