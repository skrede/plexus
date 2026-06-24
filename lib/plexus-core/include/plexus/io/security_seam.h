#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_SEAM_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_SEAM_H

#include "plexus/io/security/attach_facts.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::io {

// The negotiated, plaintext attach context the bridge hands the security seam at the
// accept edge: the wire fields the decorator's key schedule needs. It carries NO
// OpenSSL type — the gated install functor derives the keys and instantiates the AEAD
// decorator behind the erased boundary, so the core bridge never includes libcrypto.
struct security_negotiation
{
    std::array<std::byte, security::k_key_id_len> key_id{};
    std::array<std::byte, 16>                     initiator_nonce{};
    std::array<std::byte, 16>                     responder_nonce{};
    std::array<std::byte, 32>                     transcript_digest{};
    security::attach_role                         role{};
    std::uint8_t                                  chosen_cipher{0};
};

// The type-erased, OpenSSL-free security seam injected once at session-spine
// construction (the single node-level seam). transcript binds the negotiation transcript
// into a digest (the facts the gate decides on, and the key schedule's info); an empty
// transcript is the no-AEAD posture (the digest stays zeroed and the accept-any path is
// unchanged). The install functor is set PER-SESSION by the registry from this seam plus
// the just-built channel — the seam itself is node-level config; the per-session capture
// lives on peer_session's settable hook.
struct security_seam
{
    // Fold the negotiation (cipher offer/chosen, protocol version, both nonces) into a
    // 32-byte digest. Empty = no security posture engaged. Held mutable: the C++20
    // fallback move_only_function has a non-const call operator, and the seam is borrowed
    // const by every session (mirroring cookie_secret's mutable hmac seam).
    mutable plexus::detail::move_only_function<bool(std::span<const std::byte> transcript, std::span<std::byte, 32> out)> transcript;

    [[nodiscard]] bool engaged() const noexcept
    {
        return static_cast<bool>(transcript);
    }

    [[nodiscard]] bool compute(std::span<const std::byte> in, std::span<std::byte, 32> out) const
    {
        return transcript(in, out);
    }
};

}

#endif
