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

// Single-byte protocol version: the EXACT-match hard gate. A peer whose
// protocol_version differs from this constant is rejected outright — there is no
// negotiation, so a skewed peer is never silently downgraded. The two-tier
// version model layers a compatibility window (major/minor) on top of this gate.
constexpr std::uint8_t k_protocol_version = 7;

// Wire-stable handshake status byte. Integers are append-only: a value is NEVER
// reordered or reused; a new status takes the next free integer. rejected_unknown
// reserves room for an as-yet-unmapped rejection so a later addition stays
// additive rather than an enum renumbering.
enum class handshake_status : std::uint8_t
{
    accepted             = 0x01,
    version_incompatible = 0x02,
    identity_conflict    = 0x03,
    rejected_unknown     = 0x04,
    unauthorized         = 0x05
};

// Compile-time pins: each enumerator is bound to its source integer so a rename
// can never silently move a wire byte off its value.
static_assert(static_cast<std::uint8_t>(handshake_status::accepted) == 0x01);
static_assert(static_cast<std::uint8_t>(handshake_status::version_incompatible) == 0x02);
static_assert(static_cast<std::uint8_t>(handshake_status::identity_conflict) == 0x03);
static_assert(static_cast<std::uint8_t>(handshake_status::rejected_unknown) == 0x04);
static_assert(static_cast<std::uint8_t>(handshake_status::unauthorized) == 0x05);

// The opaque key-id width carried in the pre-AEAD plaintext attach region (8 random
// bytes — never a meaningful name, RFC 9257 PSK-identity privacy). This wire-local
// constant mirrors the core attach_facts width; the bridge maps the two without this
// header taking an upward dependency on the core.
constexpr std::size_t k_handshake_key_id_len = 8;

// The offered-AEAD-cipher bitmask carried alongside the key-id: a fixed-width byte
// (NOT a variable-length list) so the decode stays bounds-trivial. chosen_cipher
// echoes the single negotiated bit. Binding this offer into the attach proof's
// transcript digest makes a stripped offer change the recomputed MAC (downgrade
// refusal).
namespace cipher_offer_bits {
constexpr std::uint8_t chacha20_poly1305 = 0x01;
constexpr std::uint8_t aes_256_gcm       = 0x02;
}

// The attach challenge-response MAC width carried in the pre-AEAD plaintext region:
// 32 bytes, the HMAC-SHA256 output the attach_policy recomputes and constant-time
// compares (mirrors the core attach proof width). The verifier rejects a wrong/absent
// proof, so the field is REQUIRED (not safely-ignorable) — its addition bumps the
// protocol version.
constexpr std::size_t k_handshake_proof_len = 32;

// The id field is the RAW std::array<std::byte, 16> rather than plexus::node_id so
// this header keeps plexus-wire's zero-upward-dependency (it must not depend on the
// core). Alias transparency means the core-side FSM passes this array wherever a
// plexus::node_id is expected with no conversion.
// The same-host fingerprint is the RAW std::uint64_t value rather than the core
// host_fingerprint type, for the same reason id carries the raw std::array: this
// header keeps plexus-wire's zero-upward-dependency (it must not include the core
// io/shm header). The core-side FSM/session sets it from host_fingerprint::value
// and compares the decoded value against its own. Append-only: it sits at the next
// free offset AFTER protocol_version, so a 0 (the null / not-same-host value)
// decodes from a field-zeroed frame. The wire byte order is the codec's standard
// big-endian (write_u64/read_u64), so the value crosses hosts identically.
struct handshake_request
{
    std::array<std::byte, 16>                     id;
    std::uint8_t                                  version_major;
    std::uint8_t                                  version_minor;
    std::uint8_t                                  compatible_version_major;
    std::uint8_t                                  compatible_version_minor;
    std::uint8_t                                  protocol_version;
    std::uint64_t                                 fingerprint;
    std::array<std::byte, k_handshake_key_id_len> key_id;
    std::array<std::byte, 16>                     own_nonce;
    std::uint8_t                                  cipher_offer;
    std::uint8_t                                  chosen_cipher;
    std::array<std::byte, k_handshake_proof_len>  proof;
};

struct handshake_response
{
    std::array<std::byte, 16>                     id;
    std::uint8_t                                  version_major;
    std::uint8_t                                  version_minor;
    std::uint8_t                                  compatible_version_major;
    std::uint8_t                                  compatible_version_minor;
    std::uint8_t                                  protocol_version;
    std::uint64_t                                 fingerprint;
    std::array<std::byte, k_handshake_key_id_len> key_id;
    std::array<std::byte, 16>                     own_nonce;
    std::uint8_t                                  cipher_offer;
    std::uint8_t                                  chosen_cipher;
    std::array<std::byte, k_handshake_proof_len>  proof;
    handshake_status                              status;
};

// FIXED wire size of an encoded handshake_request: id(16) + 5 single-byte fields +
// fingerprint(8) + the appended attach region key_id(8) + own_nonce(16) +
// cipher_offer(1) + chosen_cipher(1) + proof(32) = 87.
constexpr std::size_t handshake_request_size = 87;
// FIXED wire size of an encoded handshake_response: the request 87 + status(1). The
// status stays the LAST byte (after the appended attach region + proof), so the
// request+1 relation is preserved.
constexpr std::size_t handshake_response_size = 88;

static_assert(handshake_response_size == handshake_request_size + 1);

}

// The encode/decode codecs over the shapes above are relocated to detail/handshake_codec.h; the
// include keeps every wire::encode_handshake_* / decode_handshake_* call site resolving unchanged.
#include "plexus/wire/detail/handshake_codec.h"

#endif
