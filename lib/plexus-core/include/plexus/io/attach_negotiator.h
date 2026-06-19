#ifndef HPP_GUARD_PLEXUS_IO_ATTACH_NEGOTIATOR_H
#define HPP_GUARD_PLEXUS_IO_ATTACH_NEGOTIATOR_H

#include "plexus/io/host_identity.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/security_event.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"
#include "plexus/io/security/cookie_secret.h"

#include "plexus/node_id.h"

#include "plexus/wire/handshake.h"

#include "plexus/detail/compat.h"

#include <array>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

// >200 LOC: the security-contract documentation (the nonce-once / anti-reflection /
// transcript-downgrade / fail-closed-posture invariants) is the bulk; the code is ~145 lines.
//
// The security/transcript/proof orchestrator the peer_session owns BY VALUE and drives
// (composition — no inheritance, no shared_from_this, no static state). It owns the
// per-session attach credentials and the pending attach context, borrows the node-level
// security_seam, and runs the PSK attach-proof assembly, the transcript-bound downgrade
// digest, the role-mirrored anti-reflection nonce assignment, the fail-closed posture
// gate, and the security-engaged AEAD install. It computes no crypto itself: the digest
// folds through the injected seam and the proof through the injected prover, so the core
// bridge stays OpenSSL-free. The bridge forwards its security setters here and feeds the
// negotiator the few bridge facts each step needs (self_id, policy engagement, the
// channel scheme); the wire assembly of the request/response stays in the bridge.
template<typename Policy>
class attach_negotiator
{
public:
    // The bridge-assembled attach context: the facts the gate decides on plus the
    // negotiation the AEAD key schedule needs. Both are filled from the SAME decoded
    // wire region so the digest the gate bound is the digest the keys derive from.
    struct pending_attach
    {
        security::attach_facts facts{};
        security_negotiation   negotiation{};
        bool                   engaged{false};
    };

    void
    on_install_security(plexus::detail::move_only_function<void(const security_negotiation &)> cb)
    {
        m_install_security = std::move(cb);
    }
    void set_security_seam(const security_seam *seam) noexcept { m_install_security_seam = seam; }
    void set_attach_entropy(security::rand_fn rand) { m_rand = std::move(rand); }
    void set_attach_prover(security::attach_prover prover) { m_prover = std::move(prover); }

    std::optional<node_id> authenticated_host_identity() const noexcept
    {
        return m_authenticated_host_identity;
    }

    // Mint this session's own_nonce ONCE (a fresh per-session salt for the key schedule)
    // and adopt the prover's key_id when an attach prover is engaged. Idempotent: a second
    // start() (a reconnect cycle) re-mints a fresh nonce, but a single cycle fills exactly
    // once so the SAME value feeds self_request, the transcript, and the proof. A degraded
    // RNG (rand_fn returns false) leaves the nonce zeroed on a best-effort basis — the
    // engaged crypto path additionally depends on the seam being installed, which the
    // deployment owns. The accept-any path (no rand_fn) leaves the nonce zero unchanged.
    void prime() noexcept
    {
        if(m_rand)
            (void)m_rand(m_own_nonce);
        if(m_prover.engaged())
            m_key_id = m_prover.key_id();
    }

    const std::array<std::byte, 16> &own_nonce() const noexcept { return m_own_nonce; }
    const std::array<std::byte, wire::k_handshake_key_id_len> &key_id() const noexcept
    {
        return m_key_id;
    }
    std::uint8_t cipher_offer() const noexcept
    {
        return seam_engaged() ? wire::cipher_offer_bits::chacha20_poly1305 : 0;
    }

    const pending_attach         &pending() const noexcept { return m_pending_attach; }
    const security::attach_facts &facts() const noexcept { return m_pending_attach.facts; }

    bool seam_engaged() const noexcept
    {
        return m_install_security_seam != nullptr && m_install_security_seam->engaged();
    }

    // Distinguish WHY the attach gate refused so the security event carries the cause an
    // observer acts on. A posture mismatch is caught earlier (posture_mismatched ->
    // refuse_posture), so a gate refusal on a SECURED pair is a transcript-bound proof
    // failure — a forced downgrade (a stripped/forced cipher offer changed the digest the
    // proof covered). A refusal with no security posture is a plain unauthorized attach.
    security_kind classify_unauthorized() const noexcept
    {
        return m_pending_attach.engaged ? security_kind::downgrade_refused
                                        : security_kind::unauthorized_attach;
    }

    // Assemble the attach facts + negotiation from a decoded handshake frame (request or
    // response — both carry the identical attach region) and latch them as the pending
    // attach. The peer's own_nonce is THIS side's verifier challenge in reverse: the peer
    // chose it, so from the local node's standpoint it is the peer_nonce the proof must
    // cover (anti-reflection). The transcript digest folds the negotiation through the
    // injected (OpenSSL-free) seam; a disengaged seam leaves the digest zeroed and the
    // accept-any path unchanged. The initiator/responder ids are pinned by role: the local
    // node is one, the peer the other. No crypto runs here — the proof recompute lives in
    // the policy, the key derivation behind the install hook.
    template<typename Frame>
    void assemble(const Frame &frame, security::attach_role local_role, const node_id &peer_id,
                  const node_id &self_id, bool policy_engaged)
    {
        pending_attach out;
        out.engaged                   = policy_engaged && seam_engaged();
        out.negotiation.key_id        = frame.key_id;
        out.negotiation.role          = local_role;
        out.negotiation.chosen_cipher = frame.chosen_cipher;
        // The local node's own challenge rides own_nonce; the peer's rides the decoded
        // frame. initiator/responder nonces are assigned by the local role.
        out.negotiation.initiator_nonce =
                local_role == security::attach_role::initiator ? m_own_nonce : frame.own_nonce;
        out.negotiation.responder_nonce =
                local_role == security::attach_role::initiator ? frame.own_nonce : m_own_nonce;
        out.facts.key_id       = frame.key_id;
        out.facts.initiator_id = local_role == security::attach_role::initiator ? self_id : peer_id;
        out.facts.responder_id = local_role == security::attach_role::initiator ? peer_id : self_id;
        out.facts.peer_nonce   = frame.own_nonce;
        out.facts.own_nonce    = m_own_nonce;
        out.facts.role         = local_role;
        if(out.engaged)
            compute_transcript(out, frame);
        out.negotiation.transcript_digest = out.facts.transcript_digest;
        m_pending_attach                  = out;
    }

    // Latch the decoded wire proof into a stable member buffer and point facts.proof at
    // it: the policy's ct_equal reads the span THROUGH the gate's decide() call, and a span
    // into the transient decoded frame would dangle. The buffer is the negotiator's, so it
    // outlives the synchronous gate. Called from the receive handlers AFTER the facts are
    // assembled, before the FSM gate runs.
    template<typename Frame>
    void latch_peer_proof(const Frame &frame)
    {
        m_peer_proof                 = frame.proof;
        m_pending_attach.facts.proof = m_peer_proof;
    }

    // The posture gate: a secured node (an engaged seam) and a plain peer (no
    // cipher offered) are a hard mismatch BOTH ways — a secured local meeting a plain
    // offer, OR a plain local meeting a secured offer — refused fail-closed with no
    // silent plaintext fallback. The peer's posture is read from cipher_offer (a non-zero
    // offer means it proposes AEAD). This runs ahead of the FSM accept so a mismatched
    // pair never resolves.
    template<typename Frame>
    bool posture_mismatched(const Frame &frame) const noexcept
    {
        const bool local_secured = seam_engaged();
        const bool peer_secured  = frame.cipher_offer != 0;
        return local_secured != peer_secured;
    }

    // Compute the proof the dialer verifies: the responder reconstructs the DIALER's
    // attach_facts view (the dialer is the initiator, so its own_nonce is the responder's
    // peer_nonce and vice-versa) and MACs the canonical attach_proof_input under the shared
    // PSK. The pending attach (assembled on this side's on_request) already carries the
    // role-mirrored ids/nonces/transcript for the peer, so the dialer-view facts are that
    // value with the role and nonces swapped to the dialer's standpoint. A disengaged
    // prover returns a zero proof (the accept-any path leaves the field unused).
    std::array<std::byte, wire::k_handshake_proof_len> response_proof() const
    {
        std::array<std::byte, wire::k_handshake_proof_len> proof{};
        if(!m_prover.engaged())
            return proof;
        security::attach_facts dialer_view = m_pending_attach.facts;
        dialer_view.role                   = security::attach_role::initiator;
        std::swap(dialer_view.peer_nonce, dialer_view.own_nonce);
        (void)m_prover.prove(dialer_view, proof);
        return proof;
    }

    // On a security-engaged accept, latch the authenticated host identity from the
    // VERIFIED facts (never a wire claim) and install the AEAD decorator over a plaintext
    // network channel. A transport that already authenticates-encrypts (a TLS/DTLS
    // channel) is NOT double-wrapped — the attach still bound the identity, but no AEAD
    // decorator is installed. The install hook runs before the forwarders so subsequent
    // frames are sealed; the bridge holds the erased channel and the gated layer owns the
    // EVP instantiation.
    // Returns false on a fail-closed posture refusal: a security-engaged accept over ANY
    // plaintext network channel (stream OR datagram) with no AEAD install hook is refused,
    // never silently proceeded — without the decorator a secured posture would transmit
    // cleartext while reporting a latched authenticated identity. The two transports fail
    // the same way; there is no stream-only fail-open exception.
    bool install_on_accept(bool channel_is_self_securing)
    {
        if(!m_pending_attach.engaged)
            return true;
        m_authenticated_host_identity = authenticated_peer_id(m_pending_attach.facts);
        if(channel_is_self_securing)
            return true;
        if(!m_install_security)
            return false;
        m_install_security(m_pending_attach.negotiation);
        return true;
    }

    void clear_identity() noexcept { m_authenticated_host_identity.reset(); }
    bool engaged() const noexcept { return m_pending_attach.engaged; }

private:
    // Fold the negotiation transcript (cipher_offer ‖ chosen ‖ protocol_version ‖ both
    // nonces) into the facts' digest via the seam's injected hash. The transcript bytes
    // are assembled in a fixed order both ends agree on; a stripped/forced offer changes
    // the digest, so the gate's recomputed proof — and the derived keys — differ
    // (downgrade refusal).
    template<typename Frame>
    void compute_transcript(pending_attach &out, const Frame &frame) const
    {
        std::array<std::byte, 1 + 1 + 1 + 16 + 16> transcript{};
        std::size_t                                n = 0;
        transcript[n++]                              = static_cast<std::byte>(frame.cipher_offer);
        transcript[n++]                              = static_cast<std::byte>(frame.chosen_cipher);
        transcript[n++] = static_cast<std::byte>(wire::k_protocol_version);
        for(auto b : out.negotiation.initiator_nonce)
            transcript[n++] = b;
        for(auto b : out.negotiation.responder_nonce)
            transcript[n++] = b;
        std::array<std::byte, 32> digest{};
        if(m_install_security_seam->compute(transcript, digest))
            out.facts.transcript_digest = digest;
    }

    plexus::detail::move_only_function<void(const security_negotiation &)> m_install_security;
    const security_seam   *m_install_security_seam{nullptr};
    pending_attach         m_pending_attach;
    std::optional<node_id> m_authenticated_host_identity;
    // The per-session attach credentials: own_nonce is minted once from the rand_fn seam
    // (a fresh key-schedule salt per session — no all-zero nonce), key_id is adopted from
    // the engaged prover. m_peer_proof is the stable backing buffer the decoded wire proof
    // is latched into so the facts.proof span outlives the synchronous gate decide().
    std::array<std::byte, 16>                           m_own_nonce{};
    std::array<std::byte, wire::k_handshake_key_id_len> m_key_id{};
    std::array<std::byte, wire::k_handshake_proof_len>  m_peer_proof{};
    security::rand_fn                                   m_rand;
    security::attach_prover                             m_prover;
};

}

#endif
