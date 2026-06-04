#ifndef HPP_GUARD_PLEXUS_WIRE_HANDSHAKE_H
#define HPP_GUARD_PLEXUS_WIRE_HANDSHAKE_H

#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

// Single-byte protocol version: the EXACT-match hard gate. A peer whose
// protocol_version differs from this constant is rejected outright — there is no
// negotiation, so a skewed peer is never silently downgraded. The two-tier
// version model layers a compatibility window (major/minor) on top of this gate.
constexpr std::uint8_t k_protocol_version = 1;

// Wire-stable handshake status byte. Integers are append-only: a value is NEVER
// reordered or reused; a new status takes the next free integer. rejected_unknown
// reserves room for an as-yet-unmapped rejection so a later addition stays
// additive rather than an enum renumbering.
enum class handshake_status : std::uint8_t
{
    accepted             = 0x01,
    version_incompatible = 0x02,
    identity_conflict    = 0x03,
    rejected_unknown     = 0x04
};

// Compile-time pins: each enumerator is bound to its source integer so a rename
// can never silently move a wire byte off its value.
static_assert(static_cast<std::uint8_t>(handshake_status::accepted) == 0x01);
static_assert(static_cast<std::uint8_t>(handshake_status::version_incompatible) == 0x02);
static_assert(static_cast<std::uint8_t>(handshake_status::identity_conflict) == 0x03);
static_assert(static_cast<std::uint8_t>(handshake_status::rejected_unknown) == 0x04);

// The id field is the RAW std::array<std::byte, 16> rather than plexus::node_id so
// this header keeps plexus-wire's zero-upward-dependency (it must not depend on the
// core). Alias transparency means the core-side FSM passes this array wherever a
// plexus::node_id is expected with no conversion.
struct handshake_request
{
    std::array<std::byte, 16> id;
    std::uint8_t              version_major;
    std::uint8_t              version_minor;
    std::uint8_t              compatible_version_major;
    std::uint8_t              compatible_version_minor;
    std::uint8_t              protocol_version;
};

struct handshake_response
{
    std::array<std::byte, 16> id;
    std::uint8_t              version_major;
    std::uint8_t              version_minor;
    std::uint8_t              compatible_version_major;
    std::uint8_t              compatible_version_minor;
    std::uint8_t              protocol_version;
    handshake_status          status;
};

}

#endif
