#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ATTACH_RESOLVE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ATTACH_RESOLVE_H

#include "plexus/io/security_seam.h"

#include "plexus/io/security/attach_facts.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include <array>
#include <cstddef>
#include <utility>

namespace plexus::io::detail {

// Fold the transcript (cipher_offer ‖ chosen ‖ protocol_version ‖ both nonces, in this fixed
// order) into the digest: a stripped/forced offer changes the digest, hence the recomputed
// proof and derived keys (downgrade refusal).
template<typename Frame>
void compute_transcript(const security_seam &seam, security::attach_facts &facts, const security_negotiation &negotiation, const Frame &frame)
{
    std::array<std::byte, 1 + 1 + 1 + 16 + 16> transcript{};
    std::size_t n   = 0;
    transcript[n++] = static_cast<std::byte>(frame.cipher_offer);
    transcript[n++] = static_cast<std::byte>(frame.chosen_cipher);
    transcript[n++] = static_cast<std::byte>(wire::k_protocol_version);
    for(auto b : negotiation.initiator_nonce)
        transcript[n++] = b;
    for(auto b : negotiation.responder_nonce)
        transcript[n++] = b;
    std::array<std::byte, 32> digest{};
    if(seam.compute(transcript, digest))
        facts.transcript_digest = digest;
}

// The peer's own_nonce is, locally, the peer_nonce the proof must cover (anti-reflection);
// initiator/responder ids and nonces are pinned by local role.
template<typename Pending, typename Frame>
void assemble_pending(Pending &out, const std::array<std::byte, 16> &own_nonce, const security_seam *seam, bool engaged, const Frame &frame, security::attach_role local_role,
                      const node_id &peer_id, const node_id &self_id)
{
    const bool initiator            = local_role == security::attach_role::initiator;
    out.engaged                     = engaged;
    out.negotiation.key_id          = frame.key_id;
    out.negotiation.role            = local_role;
    out.negotiation.chosen_cipher   = frame.chosen_cipher;
    out.negotiation.initiator_nonce = initiator ? own_nonce : frame.own_nonce;
    out.negotiation.responder_nonce = initiator ? frame.own_nonce : own_nonce;
    out.facts.key_id                = frame.key_id;
    out.facts.initiator_id          = initiator ? self_id : peer_id;
    out.facts.responder_id          = initiator ? peer_id : self_id;
    out.facts.peer_nonce            = frame.own_nonce;
    out.facts.own_nonce             = own_nonce;
    out.facts.role                  = local_role;
    if(out.engaged)
        compute_transcript(*seam, out.facts, out.negotiation, frame);
    out.negotiation.transcript_digest = out.facts.transcript_digest;
}

// Reconstruct the dialer's attach_facts view (role and nonces swapped to its standpoint) and
// MAC it under the shared PSK. A disengaged prover returns a zero proof.
template<typename Prover>
std::array<std::byte, wire::k_handshake_proof_len> response_proof(const Prover &prover, security::attach_facts dialer_view)
{
    std::array<std::byte, wire::k_handshake_proof_len> proof{};
    if(!prover.engaged())
        return proof;
    dialer_view.role = security::attach_role::initiator;
    std::swap(dialer_view.peer_nonce, dialer_view.own_nonce);
    (void)prover.prove(dialer_view, proof);
    return proof;
}

}

#endif
