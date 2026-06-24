#ifndef HPP_GUARD_PLEXUS_CRYPTO_KEY_SCHEDULE_H
#define HPP_GUARD_PLEXUS_CRYPTO_KEY_SCHEDULE_H

#include "plexus/crypto/aead_cipher.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::crypto {

// The per-direction message count at which the channel MUST rekey before the AEAD
// usage approaches the safety bound (RFC 9147 "rekey prior to allowing the sequence
// number to wrap"). The practical trigger is a usage threshold well below the NIST
// AES-GCM ~2^32-invocation-per-key ballpark, NOT the astronomically-far 96-bit nonce
// wrap. Set from a recorded grid sweep (the rekey-threshold test sweeps {2^18, 2^20,
// 2^22, 2^24, 2^26}): every cell has zero (key,nonce) reuse under the deterministic
// per-epoch monotonic counter; the margin to the 2^32 bound runs 2^14 (at 2^18) down
// to 2^6 (at 2^26). 2^20 is the cell that keeps a large 2^12 margin to the safety
// bound (far beyond the 2^6 floor) while giving a sane rekey cadence for a control-loop
// workload (~17.5 min between rekeys at a 1 kHz per-direction message rate).
constexpr std::uint64_t k_rekey_message_threshold = 1ull << 20;

// The NIST AES-GCM per-key invocation ballpark the threshold sits below.
constexpr std::uint64_t k_aead_safety_bound = 1ull << 32;

// The per-direction keys derived once per session. Direction separation (k_send !=
// k_recv) follows the TLS 1.3 client/server-key discipline: a single symmetric key
// both ways invites the Selfie reflection (eprint 2019/347).
struct derived_keys
{
    aead_key k_send{};
    aead_key k_recv{};
};

// HKDF-SHA256 (RFC 5869) derivation policy.
//   master = Extract(salt = initiator_nonce || responder_nonce, ikm = psk)
//   k_send = Expand(master, info = "plexus aead snd" || transcript_digest)
//   k_recv = Expand(master, info = "plexus aead rcv" || transcript_digest)
// The transcript_digest binds the negotiation (cipher offer, chosen cipher,
// protocol version, both nonces); a tampered offer derives different keys, so the
// downgrade fails closed at the first AEAD frame. Its assembly is the handshake
// bridge's job — it is PASSED IN here.
bool derive_keys(std::span<const std::byte> psk, std::span<const std::byte, 16> initiator_nonce, std::span<const std::byte, 16> responder_nonce,
                 std::span<const std::byte, 32> transcript_digest, derived_keys &out);

}

#endif
