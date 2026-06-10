#ifndef HPP_GUARD_PLEXUS_WIRE_HANDSHAKE_H
#define HPP_GUARD_PLEXUS_WIRE_HANDSHAKE_H

#include "plexus/wire/byte_order.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace plexus::wire {

// Single-byte protocol version: the EXACT-match hard gate. A peer whose
// protocol_version differs from this constant is rejected outright — there is no
// negotiation, so a skewed peer is never silently downgraded. The two-tier
// version model layers a compatibility window (major/minor) on top of this gate.
constexpr std::uint8_t k_protocol_version = 6;

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
    std::array<std::byte, 16> id;
    std::uint8_t              version_major;
    std::uint8_t              version_minor;
    std::uint8_t              compatible_version_major;
    std::uint8_t              compatible_version_minor;
    std::uint8_t              protocol_version;
    std::uint64_t             fingerprint;
    std::array<std::byte, k_handshake_key_id_len> key_id;
    std::array<std::byte, 16> own_nonce;
    std::uint8_t              cipher_offer;
    std::uint8_t              chosen_cipher;
    std::array<std::byte, k_handshake_proof_len> proof;
};

struct handshake_response
{
    std::array<std::byte, 16> id;
    std::uint8_t              version_major;
    std::uint8_t              version_minor;
    std::uint8_t              compatible_version_major;
    std::uint8_t              compatible_version_minor;
    std::uint8_t              protocol_version;
    std::uint64_t             fingerprint;
    std::array<std::byte, k_handshake_key_id_len> key_id;
    std::array<std::byte, 16> own_nonce;
    std::uint8_t              cipher_offer;
    std::uint8_t              chosen_cipher;
    std::array<std::byte, k_handshake_proof_len> proof;
    handshake_status          status;
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

// Decode cutoff for the response status byte. The switch enumerates ONLY the
// defined values; an out-of-range byte or a reserved in-range gap matches no case,
// returns false, and the frame is rejected — so no undefined handshake_status
// enumerator is ever constructed.
inline bool is_defined_handshake_status(std::uint8_t byte) noexcept
{
    switch(static_cast<handshake_status>(byte))
    {
    case handshake_status::accepted:
    case handshake_status::version_incompatible:
    case handshake_status::identity_conflict:
    case handshake_status::rejected_unknown:
    case handshake_status::unauthorized:
        return true;
    }
    return false;
}

inline void encode_handshake_request_into(std::vector<std::byte> &out, const handshake_request &req)
{
    out.resize(handshake_request_size);
    auto *p = out.data();
    std::memcpy(p, req.id.data(), req.id.size()); // 16 bytes verbatim (opaque)
    detail::write_u8(p + 16, req.version_major);
    detail::write_u8(p + 17, req.version_minor);
    detail::write_u8(p + 18, req.compatible_version_major);
    detail::write_u8(p + 19, req.compatible_version_minor);
    detail::write_u8(p + 20, req.protocol_version);
    detail::write_u64(p + 21, req.fingerprint);
    std::memcpy(p + 29, req.key_id.data(), req.key_id.size());       // 8 bytes opaque
    std::memcpy(p + 37, req.own_nonce.data(), req.own_nonce.size()); // 16 bytes opaque
    detail::write_u8(p + 53, req.cipher_offer);
    detail::write_u8(p + 54, req.chosen_cipher);
    std::memcpy(p + 55, req.proof.data(), req.proof.size()); // 32 bytes opaque MAC
}

inline void encode_handshake_response_into(std::vector<std::byte> &out, const handshake_response &resp)
{
    out.resize(handshake_response_size);
    auto *p = out.data();
    std::memcpy(p, resp.id.data(), resp.id.size()); // 16 bytes verbatim (opaque)
    detail::write_u8(p + 16, resp.version_major);
    detail::write_u8(p + 17, resp.version_minor);
    detail::write_u8(p + 18, resp.compatible_version_major);
    detail::write_u8(p + 19, resp.compatible_version_minor);
    detail::write_u8(p + 20, resp.protocol_version);
    detail::write_u64(p + 21, resp.fingerprint);
    std::memcpy(p + 29, resp.key_id.data(), resp.key_id.size());       // 8 bytes opaque
    std::memcpy(p + 37, resp.own_nonce.data(), resp.own_nonce.size()); // 16 bytes opaque
    detail::write_u8(p + 53, resp.cipher_offer);
    detail::write_u8(p + 54, resp.chosen_cipher);
    std::memcpy(p + 55, resp.proof.data(), resp.proof.size()); // 32 bytes opaque MAC
    detail::write_u8(p + 87, static_cast<std::uint8_t>(resp.status));
}

inline std::vector<std::byte> encode_handshake_request(const handshake_request &req)
{
    std::vector<std::byte> out;
    encode_handshake_request_into(out, req);
    return out;
}

inline std::vector<std::byte> encode_handshake_response(const handshake_response &resp)
{
    std::vector<std::byte> out;
    encode_handshake_response_into(out, resp);
    return out;
}

inline std::optional<handshake_request> decode_handshake_request(std::span<const std::byte> payload)
{
    if(payload.size() < handshake_request_size)
        return std::nullopt;

    handshake_request req{};
    const auto *p = payload.data();
    std::memcpy(req.id.data(), p, req.id.size()); // 16 bytes verbatim (opaque)
    req.version_major            = detail::read_u8(p + 16);
    req.version_minor            = detail::read_u8(p + 17);
    req.compatible_version_major = detail::read_u8(p + 18);
    req.compatible_version_minor = detail::read_u8(p + 19);
    req.protocol_version         = detail::read_u8(p + 20);
    req.fingerprint              = detail::read_u64(p + 21);
    std::memcpy(req.key_id.data(), p + 29, req.key_id.size());       // 8 bytes verbatim
    std::memcpy(req.own_nonce.data(), p + 37, req.own_nonce.size()); // 16 bytes verbatim
    req.cipher_offer             = detail::read_u8(p + 53);
    req.chosen_cipher            = detail::read_u8(p + 54);
    std::memcpy(req.proof.data(), p + 55, req.proof.size()); // 32 bytes verbatim
    return req;
}

inline std::optional<handshake_response> decode_handshake_response(std::span<const std::byte> payload)
{
    if(payload.size() < handshake_response_size)
        return std::nullopt;

    const auto *p = payload.data();
    auto status_byte = detail::read_u8(p + 87);
    if(!is_defined_handshake_status(status_byte))
        return std::nullopt;

    handshake_response resp{};
    std::memcpy(resp.id.data(), p, resp.id.size()); // 16 bytes verbatim (opaque)
    resp.version_major            = detail::read_u8(p + 16);
    resp.version_minor            = detail::read_u8(p + 17);
    resp.compatible_version_major = detail::read_u8(p + 18);
    resp.compatible_version_minor = detail::read_u8(p + 19);
    resp.protocol_version         = detail::read_u8(p + 20);
    resp.fingerprint              = detail::read_u64(p + 21);
    std::memcpy(resp.key_id.data(), p + 29, resp.key_id.size());       // 8 bytes verbatim
    std::memcpy(resp.own_nonce.data(), p + 37, resp.own_nonce.size()); // 16 bytes verbatim
    resp.cipher_offer             = detail::read_u8(p + 53);
    resp.chosen_cipher            = detail::read_u8(p + 54);
    std::memcpy(resp.proof.data(), p + 55, resp.proof.size()); // 32 bytes verbatim
    resp.status                   = static_cast<handshake_status>(status_byte);
    return resp;
}

}

#endif
