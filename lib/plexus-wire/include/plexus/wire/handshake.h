#ifndef HPP_GUARD_PLEXUS_WIRE_HANDSHAKE_H
#define HPP_GUARD_PLEXUS_WIRE_HANDSHAKE_H

#include "plexus/wire/cursor.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// Single-byte protocol version: the EXACT-match hard gate. A peer whose protocol_version
// differs is rejected outright — no negotiation. A compatibility window (major/minor) layers
// on top of this gate.
constexpr std::uint8_t k_protocol_version = 7;

// Wire-stable handshake status byte. Append-only: a value is NEVER reordered or reused; a new
// status takes the next free integer.
enum class handshake_status : std::uint8_t
{
    accepted             = 0x01,
    version_incompatible = 0x02,
    identity_conflict    = 0x03,
    rejected_unknown     = 0x04,
    unauthorized         = 0x05
};

// Pin each enumerator to its wire integer so a rename can never silently move a wire byte.
static_assert(static_cast<std::uint8_t>(handshake_status::accepted) == 0x01);
static_assert(static_cast<std::uint8_t>(handshake_status::version_incompatible) == 0x02);
static_assert(static_cast<std::uint8_t>(handshake_status::identity_conflict) == 0x03);
static_assert(static_cast<std::uint8_t>(handshake_status::rejected_unknown) == 0x04);
static_assert(static_cast<std::uint8_t>(handshake_status::unauthorized) == 0x05);

// The opaque key-id width in the pre-AEAD plaintext attach region: 8 random bytes, never a
// meaningful name (RFC 9257 PSK-identity privacy).
constexpr std::size_t k_handshake_key_id_len = 8;

// The offered-AEAD-cipher bitmask: a fixed-width byte (NOT a variable-length list) so the
// decode stays bounds-trivial. Binding this offer into the attach proof's transcript digest
// makes a stripped offer change the recomputed MAC (downgrade refusal).
namespace cipher_offer_bits {
constexpr std::uint8_t chacha20_poly1305 = 0x01;
constexpr std::uint8_t aes_256_gcm       = 0x02;
}

// The attach challenge-response MAC width: 32 bytes, the HMAC-SHA256 output the verifier
// recomputes and constant-time compares. REQUIRED, not safely-ignorable — its addition bumps
// the protocol version.
constexpr std::size_t k_handshake_proof_len = 32;

// id is the RAW std::array<std::byte, 16> (not plexus::node_id) and fingerprint the RAW
// std::uint64_t (not host_fingerprint) so this header keeps plexus-wire's zero-upward-dependency
// on the core; alias transparency lets the core-side FSM pass them through unchanged. fingerprint
// is append-only at the next free offset after protocol_version, so a field-zeroed frame decodes
// its 0 (not-same-host) value. The wire order is the codec's standard big-endian.
struct handshake_request
{
    std::array<std::byte, 16> id;
    std::uint8_t version_major;
    std::uint8_t version_minor;
    std::uint8_t compatible_version_major;
    std::uint8_t compatible_version_minor;
    std::uint8_t protocol_version;
    std::uint64_t fingerprint;
    std::array<std::byte, k_handshake_key_id_len> key_id;
    std::array<std::byte, 16> own_nonce;
    std::uint8_t cipher_offer;
    std::uint8_t chosen_cipher;
    std::array<std::byte, k_handshake_proof_len> proof;
};

struct handshake_response
{
    std::array<std::byte, 16> id;
    std::uint8_t version_major;
    std::uint8_t version_minor;
    std::uint8_t compatible_version_major;
    std::uint8_t compatible_version_minor;
    std::uint8_t protocol_version;
    std::uint64_t fingerprint;
    std::array<std::byte, k_handshake_key_id_len> key_id;
    std::array<std::byte, 16> own_nonce;
    std::uint8_t cipher_offer;
    std::uint8_t chosen_cipher;
    std::array<std::byte, k_handshake_proof_len> proof;
    handshake_status status;
};

// FIXED wire size: id(16) + 5 single-byte fields + fingerprint(8) + key_id(8) + own_nonce(16)
// + cipher_offer(1) + chosen_cipher(1) + proof(32) = 87.
constexpr std::size_t handshake_request_size = 87;
// FIXED wire size: the request 87 + the trailing status(1).
constexpr std::size_t handshake_response_size = 88;

static_assert(handshake_response_size == handshake_request_size + 1);

}

#include "plexus/wire/detail/handshake_codec.h"

#endif
