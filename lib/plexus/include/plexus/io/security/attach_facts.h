#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_FACTS_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_FACTS_H

#include "plexus/node_id.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::io::security {

// The key-id width: an opaque, random identifier carried in the pre-AEAD plaintext
// handshake region. It is never a meaningful name (RFC 9257 PSK-identity privacy) —
// 8 random bytes give a 2^64 namespace, ample for a deployment's keyed set while
// keeping the plaintext region minimal.
constexpr std::size_t k_key_id_len = 8;

// Which AEAD direction a proof was computed for. The role byte enters the proof
// input so the two directions compute distinct MACs: a reflected proof (the
// verifier's own challenge bounced back) never validates (the Selfie attack,
// eprint 2019/347).
enum class attach_role : std::uint8_t
{
    initiator,
    responder
};

// The PSK attach-decision facts the handshake bridge fills ONCE per attaching peer
// and the policy only READS — the flat-value seam mirroring cert_facts. peer_nonce
// is the VERIFIER's challenge (the reflection defense); transcript_digest binds the
// negotiated cipher offer + version so a downgrade changes the recomputed proof.
struct attach_facts
{
    std::array<std::byte, k_key_id_len> key_id{};
    node_id                             initiator_id{};
    node_id                             responder_id{};
    std::array<std::byte, 16>           peer_nonce{};
    std::array<std::byte, 16>           own_nonce{};
    std::array<std::byte, 32>           transcript_digest{};
    attach_role                         role{};
    std::span<const std::byte>          proof;
};

}

#endif
