#ifndef HPP_GUARD_PLEXUS_WIRE_SUBSCRIBE_H
#define HPP_GUARD_PLEXUS_WIRE_SUBSCRIBE_H

#include "plexus/wire/frame.h"
#include "plexus/wire/byte_order.h"
#include "plexus/wire/length_prefixed.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace plexus::wire {

enum class subscribe_status : uint8_t
{
    subscribed         = 0x01,
    created            = 0x02,
    type_mismatch      = 0x03,
    already_subscribed = 0x04,
    // A soft request-vs-offered field failed under a strict subscriber.
    incompatible_qos = 0x05,
    // The always-hard requires_source_identity field was unmet (any mode).
    source_identity_incompatible = 0x06,
    // A permissive accept whose trailing degraded_flags byte names the soft fields
    // that went unsatisfied.
    subscribed_degraded = 0x07,
    // A strict-posture subscriber attached to a producer that declared no type. Only
    // ever emitted to a subscriber that requested the strict attach posture, so an old
    // peer (which never sets that bit) never receives it (append-safe).
    type_undeclared = 0x08
};

enum class unsubscribe_status : uint8_t
{
    unsubscribed   = 0x01,
    destroyed      = 0x02,
    not_subscribed = 0x03
};

enum class subscribe_notification : uint8_t
{
    producer_available = 0x01,
    producer_gone      = 0x02
};

// The wire-layer mirror of the subscriber's QoS choices: a flat POD with the SAME field values
// as io::subscriber_qos, keeping plexus-wire's zero-upward-dependency on the core.
struct subscribe_qos_region
{
    uint8_t durability;                   // {0=none, 1=latest, 2=all}
    uint8_t delivery_mode;                // {0=push, 1=pull}
    uint32_t replay_depth;                // 0 => use the ring depth
    uint8_t requested_flags;              // bit0=requires_source_identity, bit1=requested_reliability_reliable
    uint64_t requested_deadline_ns;       // 0 = unset
    uint64_t requested_lease_ns;          // 0 = unset
    uint8_t requested_priority;           // carry-only; default band 0
    uint32_t requested_max_message_bytes; // 0 = unset = always compatible
};

struct subscribe_request
{
    std::string fqn;
    std::string type_name;
    // Three-state type assertion. type_declared=false (the default) is undeclared and type_name is
    // ignored; true with an empty type_name is declared-empty; true with a name is declared. It
    // encodes as one trailing flag byte appended AFTER the optional QoS region, so an undeclared
    // frame is byte-identical to the pre-flag layout.
    bool type_declared = false;
    uint64_t topic_hash;
    uint64_t type_hash;
    endpoint_source_type source;
    // The flag-gated trailing QoS region. has_qos=false (the default) encodes NO trailing bytes,
    // byte-identical to the pre-region layout; has_qos=true appends the fixed 30-byte region.
    bool has_qos = false;
    subscribe_qos_region qos{};
};

struct subscribe_response
{
    uint64_t topic_hash;
    subscribe_status status;
    // The optional trailing degraded-field surface. has_degraded=false (the default) encodes NO
    // trailing byte, byte-identical to the 9-byte layout; has_degraded=true (a permissive
    // subscribed_degraded accept) appends one byte carrying the unsatisfied soft-field bitmask.
    bool has_degraded           = false;
    std::uint8_t degraded_flags = 0;
};

// Wire FLOOR of an encoded subscribe_response payload: topic_hash(8) + status(1). A permissive
// degraded-accept appends an OPTIONAL 10th byte; the decoder floor stays at this size so a bare
// 9-byte response decodes unchanged.
constexpr std::size_t subscribe_response_size = 9;

struct unsubscribe_request
{
    uint64_t topic_hash;
};

struct unsubscribe_response
{
    uint64_t topic_hash;
    unsubscribe_status status;
};

namespace detail {

// Subscribe request wire layout:
//   topic_hash(8) + type_hash(8) + endpoint_source_type(1)
//   + fqn_len(2) + fqn_bytes + type_name_len(2) + type_name_bytes
//   [ + qos_region if has_qos ] [ + type_flag(1) if type_declared ]
// Minimum = 8 + 8 + 1 + 2 + 2 = 21 bytes
constexpr std::size_t subscribe_request_fixed_prefix = 17; // 8 + 8 + 1
constexpr std::size_t subscribe_request_min_size     = 21; // + 2 + 2
constexpr std::size_t unsubscribe_request_size       = 8;
constexpr std::size_t unsubscribe_response_size      = 9; // 8 + 1

// Per-callsite policy lid on the two uint16_t-prefixed string fields, both attacker-controlled:
// without it the 16-bit prefix would let a peer force a 65 KB std::string per subscribe frame.
constexpr std::size_t k_max_fqn       = 1024;
constexpr std::size_t k_max_type_name = 512;

// The trailing flag-gated subscriber-QoS region (fixed-width, behind one
// read_length_prefixed<uint16_t> guard). Present iff subscribe_request::has_qos.
//
//   durability                  u8    @ 0   {0=none, 1=latest, 2=all}
//   delivery_mode               u8    @ 1   {0=push, 1=pull}
//   replay_depth                u32   @ 2   (0 => use the ring depth)
//   requested_flags             u8    @ 6   bit0=requires_source_identity, bit1=reliable
//   requested_deadline_ns       u64   @ 7   (0 = unset)
//   requested_lease_ns          u64   @ 15  (0 = unset)
//   requested_priority          u8    @ 23  (carry-only)
//   requested_max_message_bytes u32   @ 24  (0 = unset = always compatible)
//   reserved                    u8[2] @ 28  (=0; future wire needs ride here)
// Byte-sum = 1+1+4+1+8+8+1+4+2 = 30 bytes, fixed.
constexpr std::size_t k_qos_durability_off   = 0;
constexpr std::size_t k_qos_delivery_off     = 1;
constexpr std::size_t k_qos_replay_depth_off = 2;
constexpr std::size_t k_qos_flags_off        = 6;
constexpr std::size_t k_qos_deadline_ns_off  = 7;
constexpr std::size_t k_qos_lease_ns_off     = 15;
constexpr std::size_t k_qos_priority_off     = 23;
constexpr std::size_t k_qos_max_message_off  = 24;
constexpr std::size_t k_qos_reserved_off     = 28;
constexpr std::size_t k_qos_reserved_size    = 2;

constexpr std::size_t k_qos_region_size = k_qos_reserved_off + k_qos_reserved_size;
static_assert(k_qos_region_size == 1 + 1 + 4 + 1 + 8 + 8 + 1 + 4 + 2, "the subscriber-QoS region must equal the sum of its fixed fields");
static_assert(k_qos_region_size == 30);

// Per-callsite policy lid on the attacker-controlled region length: a claimed-huge uint16_t
// prefix is refused before any read.
constexpr std::size_t k_max_qos_region = 64;

// The QoS request-flag bit allocation inside requested_flags.
constexpr std::uint8_t k_qos_flag_requires_source_identity = 0x01;
constexpr std::uint8_t k_qos_flag_requested_reliable       = 0x02;
// clear = permissive (default), set = strict.
constexpr std::uint8_t k_qos_flag_rxo_strict = 0x04;
// clear = lenient (default, attaches to an untyped producer), set = strict (refuses one with
// subscribe_status::type_undeclared).
constexpr std::uint8_t k_qos_flag_typed_strict = 0x08;

// bit0 of the optional one-byte type-declaration flag that trails the frame after the QoS region.
// set = the subscriber asserted a type (type_name carries it; an empty name is declared-empty). The
// byte is present iff a type is declared, keeping an undeclared frame byte-identical to the floor.
constexpr std::uint8_t k_type_declared_flag = 0x01;

}

}

#include "plexus/wire/detail/subscribe_codec.h"

#endif
