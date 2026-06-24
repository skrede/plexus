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

#include "plexus/io/detail/attach_resolve.h"

#include "plexus/detail/compat.h"

#include <array>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

// The security/transcript/proof orchestrator the peer_session owns BY VALUE (composition — no
// inheritance, no shared_from_this, no static state). It runs the PSK attach-proof assembly,
// the transcript-bound downgrade digest, the role-mirrored anti-reflection nonce assignment,
// the fail-closed posture gate, and the AEAD install. It computes no crypto itself — the digest
// folds through the injected seam and the proof through the injected prover, so the core bridge
// stays OpenSSL-free.
template<typename Policy>
class attach_negotiator
{
public:
    // Facts + negotiation filled from the SAME decoded wire region, so the digest the gate
    // bound is the digest the keys derive from.
    struct pending_attach
    {
        security::attach_facts facts{};
        security_negotiation   negotiation{};
        bool                   engaged{false};
    };

    void on_install_security(plexus::detail::move_only_function<void(const security_negotiation &)> cb)
    {
        m_install_security = std::move(cb);
    }
    void set_security_seam(const security_seam *seam) noexcept
    {
        m_install_security_seam = seam;
    }
    void set_attach_entropy(security::rand_fn rand)
    {
        m_rand = std::move(rand);
    }
    void set_attach_prover(security::attach_prover prover)
    {
        m_prover = std::move(prover);
    }

    std::optional<node_id> authenticated_host_identity() const noexcept
    {
        return m_authenticated_host_identity;
    }

    // Mint this session's own_nonce ONCE (a fresh key-schedule salt) so the SAME value feeds
    // self_request, the transcript, and the proof; adopt the prover's key_id when engaged. A
    // reconnect re-mints. A degraded RNG leaves the nonce zeroed (the engaged crypto path also
    // needs the seam installed, which the deployment owns).
    void prime() noexcept
    {
        if(m_rand)
            (void)m_rand(m_own_nonce);
        if(m_prover.engaged())
            m_key_id = m_prover.key_id();
    }

    const std::array<std::byte, 16> &own_nonce() const noexcept
    {
        return m_own_nonce;
    }
    const std::array<std::byte, wire::k_handshake_key_id_len> &key_id() const noexcept
    {
        return m_key_id;
    }
    std::uint8_t cipher_offer() const noexcept
    {
        return seam_engaged() ? wire::cipher_offer_bits::chacha20_poly1305 : 0;
    }

    const pending_attach &pending() const noexcept
    {
        return m_pending_attach;
    }
    const security::attach_facts &facts() const noexcept
    {
        return m_pending_attach.facts;
    }

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
        return m_pending_attach.engaged ? security_kind::downgrade_refused : security_kind::unauthorized_attach;
    }

    // Assemble facts + negotiation from a decoded handshake frame (request or response — both
    // carry the identical attach region). The peer's own_nonce is, from the local standpoint,
    // the peer_nonce the proof must cover (anti-reflection); initiator/responder ids and nonces
    // are pinned by local role. No crypto runs here.
    template<typename Frame>
    void assemble(const Frame &frame, security::attach_role local_role, const node_id &peer_id, const node_id &self_id, bool policy_engaged)
    {
        pending_attach out;
        detail::assemble_pending(out, m_own_nonce, m_install_security_seam, policy_engaged && seam_engaged(), frame, local_role, peer_id, self_id);
        m_pending_attach = out;
    }

    // Latch the wire proof into a stable member buffer (a span into the transient decoded frame
    // would dangle through the gate's decide() call). Called after assemble, before the FSM gate.
    template<typename Frame>
    void latch_peer_proof(const Frame &frame)
    {
        m_peer_proof                 = frame.proof;
        m_pending_attach.facts.proof = m_peer_proof;
    }

    // A secured node and a plain peer are a hard mismatch BOTH ways, refused fail-closed with no
    // silent plaintext fallback. Runs ahead of the FSM accept so a mismatched pair never resolves.
    template<typename Frame>
    bool posture_mismatched(const Frame &frame) const noexcept
    {
        const bool local_secured = seam_engaged();
        const bool peer_secured  = frame.cipher_offer != 0;
        return local_secured != peer_secured;
    }

    // Compute the proof the dialer verifies: reconstruct the DIALER's attach_facts view (the
    // pending attach with role and nonces swapped to the dialer's standpoint) and MAC it under
    // the shared PSK. A disengaged prover returns a zero proof (the accept-any path).
    std::array<std::byte, wire::k_handshake_proof_len> response_proof() const
    {
        return detail::response_proof(m_prover, m_pending_attach.facts);
    }

    // Latch the authenticated host identity from the VERIFIED facts (never a wire claim) and
    // install the AEAD decorator. A self-securing transport (TLS/DTLS) is NOT double-wrapped.
    // Returns false on a fail-closed posture refusal: a security-engaged accept over ANY
    // plaintext channel with no install hook is refused, never silently proceeded — without the
    // decorator a secured posture would transmit cleartext under a latched identity.
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

    void clear_identity() noexcept
    {
        m_authenticated_host_identity.reset();
    }
    bool engaged() const noexcept
    {
        return m_pending_attach.engaged;
    }

private:
    plexus::detail::move_only_function<void(const security_negotiation &)> m_install_security;
    const security_seam                                                   *m_install_security_seam{nullptr};
    pending_attach                                                         m_pending_attach;
    std::optional<node_id>                                                 m_authenticated_host_identity;
    // m_peer_proof is the stable backing buffer the wire proof is latched into so the
    // facts.proof span outlives the synchronous gate decide().
    std::array<std::byte, 16>                           m_own_nonce{};
    std::array<std::byte, wire::k_handshake_key_id_len> m_key_id{};
    std::array<std::byte, wire::k_handshake_proof_len>  m_peer_proof{};
    security::rand_fn                                   m_rand;
    security::attach_prover                             m_prover;
};

}

#endif
