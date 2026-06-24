#ifndef HPP_GUARD_PLEXUS_IO_ATTACH_NEGOTIATOR_H
#define HPP_GUARD_PLEXUS_IO_ATTACH_NEGOTIATOR_H

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include "plexus/io/host_identity.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/security_event.h"

#include "plexus/io/detail/attach_resolve.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"
#include "plexus/io/security/cookie_secret.h"

#include "plexus/wire/handshake.h"

#include <array>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

template<typename Policy>
class attach_negotiator
{
public:
    struct pending_attach
    {
        security::attach_facts facts{};
        security_negotiation negotiation{};
        bool engaged{false};
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

    // Mint own_nonce ONCE so the SAME value feeds self_request, the transcript, and the proof; a
    // degraded RNG leaves the nonce zeroed.
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

    // A gate refusal on a SECURED pair is a transcript-bound proof failure (a forced downgrade);
    // a refusal with no security posture is a plain unauthorized attach.
    security_kind classify_unauthorized() const noexcept
    {
        return m_pending_attach.engaged ? security_kind::downgrade_refused : security_kind::unauthorized_attach;
    }

    // The peer's own_nonce is, from the local standpoint, the peer_nonce the proof must cover
    // (anti-reflection); ids and nonces are pinned by local role.
    template<typename Frame>
    void assemble(const Frame &frame, security::attach_role local_role, const node_id &peer_id, const node_id &self_id, bool policy_engaged)
    {
        pending_attach out;
        detail::assemble_pending(out, m_own_nonce, m_install_security_seam, policy_engaged && seam_engaged(), frame, local_role, peer_id, self_id);
        m_pending_attach = out;
    }

    // Latch the wire proof into a stable member buffer; a span into the transient decoded frame
    // would dangle through the gate's synchronous decide().
    template<typename Frame>
    void latch_peer_proof(const Frame &frame)
    {
        m_peer_proof                 = frame.proof;
        m_pending_attach.facts.proof = m_peer_proof;
    }

    template<typename Frame>
    bool posture_mismatched(const Frame &frame) const noexcept
    {
        const bool local_secured = seam_engaged();
        const bool peer_secured  = frame.cipher_offer != 0;
        return local_secured != peer_secured;
    }

    // Reconstruct the dialer's attach_facts view (role and nonces swapped to the dialer's
    // standpoint) and MAC it under the shared PSK; a disengaged prover returns a zero proof.
    std::array<std::byte, wire::k_handshake_proof_len> response_proof() const
    {
        return detail::response_proof(m_prover, m_pending_attach.facts);
    }

    // Latch the host identity from the VERIFIED facts (never a wire claim) and install the AEAD
    // decorator; a self-securing transport is NOT double-wrapped. Returns false on a fail-closed
    // posture refusal: a security-engaged accept over a plaintext channel with no install hook
    // would otherwise transmit cleartext under a latched identity.
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
    const security_seam *m_install_security_seam{nullptr};
    pending_attach m_pending_attach;
    std::optional<node_id> m_authenticated_host_identity;
    std::array<std::byte, 16> m_own_nonce{};
    std::array<std::byte, wire::k_handshake_key_id_len> m_key_id{};
    // The stable backing buffer the wire proof is latched into so facts.proof outlives decide().
    std::array<std::byte, wire::k_handshake_proof_len> m_peer_proof{};
    security::rand_fn m_rand;
    security::attach_prover m_prover;
};

}

#endif
