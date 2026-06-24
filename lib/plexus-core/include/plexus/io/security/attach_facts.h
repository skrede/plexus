#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_FACTS_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_FACTS_H

#include "plexus/node_id.h"

#include <span>
#include <array>
#include <vector>
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
    node_id initiator_id{};
    node_id responder_id{};
    std::array<std::byte, 16> peer_nonce{};
    std::array<std::byte, 16> own_nonce{};
    std::array<std::byte, 32> transcript_digest{};
    attach_role role{};
    std::span<const std::byte> proof;
};

// The canonical attach-proof MAC input: a fixed label, the role byte, both node-ids,
// both nonces, and the transcript digest in that fixed order. The role byte makes the
// two directions compute distinct MACs (anti-reflection, the Selfie attack eprint
// 2019/347); the transcript digest makes a downgraded cipher offer change the MAC
// (anti-downgrade). Both the verifier (psk_keystore_policy) and the prover
// (peer_session) MAC over THIS exact assembly, so the recompute matches byte for byte
// — a single source for the layout, no duplicate-and-drift.
inline std::vector<std::byte> attach_proof_input(const attach_facts &f)
{
    static constexpr std::array<std::byte, 13> label{std::byte{'p'}, std::byte{'l'}, std::byte{'e'}, std::byte{'x'}, std::byte{'u'}, std::byte{'s'}, std::byte{'-'},
                                                     std::byte{'a'}, std::byte{'t'}, std::byte{'t'}, std::byte{'a'}, std::byte{'c'}, std::byte{'h'}};
    std::vector<std::byte> msg;
    msg.reserve(label.size() + 1 + f.initiator_id.size() + f.responder_id.size() + f.peer_nonce.size() + f.own_nonce.size() + f.transcript_digest.size());
    msg.insert(msg.end(), label.begin(), label.end());
    msg.push_back(static_cast<std::byte>(f.role));
    msg.insert(msg.end(), f.initiator_id.begin(), f.initiator_id.end());
    msg.insert(msg.end(), f.responder_id.begin(), f.responder_id.end());
    msg.insert(msg.end(), f.peer_nonce.begin(), f.peer_nonce.end());
    msg.insert(msg.end(), f.own_nonce.begin(), f.own_nonce.end());
    msg.insert(msg.end(), f.transcript_digest.begin(), f.transcript_digest.end());
    return msg;
}

}

#endif
