#ifndef HPP_GUARD_PLEXUS_WIRE_SUBSCRIBE_H
#define HPP_GUARD_PLEXUS_WIRE_SUBSCRIBE_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/frame.h"
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

// The wire-layer mirror of the subscriber's QoS choices. The wire layer keeps its
// zero-upward-dependency, so it carries a flat POD with the SAME field values as
// io::subscriber_qos rather than including the core header (exactly as handshake.h
// carries a raw std::array, not plexus::node_id). The core lifts this region into
// io::subscriber_qos at the decode site, packing/unpacking requested_flags.
struct subscribe_qos_region
{
    uint8_t  durability;      // {0=none, 1=latest, 2=all}
    uint8_t  delivery_mode;   // {0=push, 1=pull}
    uint32_t replay_depth;    // 0 => use the ring depth
    uint8_t  requested_flags; // bit0=requires_source_identity, bit1=requested_reliability_reliable
    uint64_t requested_deadline_ns;       // 0 = unset
    uint64_t requested_lease_ns;          // 0 = unset
    uint8_t  requested_priority;          // carry-only; default band 0
    uint32_t requested_max_message_bytes; // 0 = unset = always compatible
};

struct subscribe_request
{
    std::string          fqn;
    std::string          type_name;
    uint64_t             topic_hash;
    uint64_t             type_hash;
    endpoint_source_type source;
    // The flag-gated, trailing QoS region. has_qos=false (the default) encodes
    // NO trailing bytes, so the frame is byte-identical to the pre-region layout
    // a v3 producer would write; the decoder maps an absent region to the qos
    // defaults. has_qos=true appends the fixed 30-byte region after type_name.
    bool                 has_qos = false;
    subscribe_qos_region qos{};
};

struct subscribe_response
{
    uint64_t         topic_hash;
    subscribe_status status;
    // The optional trailing degraded-field surface (gated like subscribe_request's
    // has_qos region). has_degraded=false (the default) encodes NO trailing byte, so
    // every refusal and every clean `subscribed` stays byte-identical to the pre-extension
    // 9-byte layout a v4 peer wrote. has_degraded=true (a permissive subscribed_degraded
    // accept) appends one byte carrying the unsatisfied soft-field bitmask.
    bool         has_degraded   = false;
    std::uint8_t degraded_flags = 0;
};

// Public wire FLOOR of an encoded subscribe_response payload. Used by the
// receiver-side handler to early-validate the frame before decoding, and by
// the encoder/decoder symmetrically. 8 bytes for the topic_hash + 1 byte for
// the status enum. A permissive degraded-accept appends an OPTIONAL 10th byte;
// the decoder floor stays `< subscribe_response_size` so a bare 9-byte response
// from a v4 peer decodes unchanged and the trailing byte is simply absent.
constexpr std::size_t subscribe_response_size = 9;

struct unsubscribe_request
{
    uint64_t topic_hash;
};

struct unsubscribe_response
{
    uint64_t           topic_hash;
    unsubscribe_status status;
};

namespace detail {

// Subscribe request wire layout (no service-policy fields):
//   topic_hash(8) + type_hash(8) + endpoint_source_type(1)
//   + fqn_len(2) + fqn_bytes + type_name_len(2) + type_name_bytes
// Minimum = 8 + 8 + 1 + 2 + 2 = 21 bytes

constexpr std::size_t subscribe_request_fixed_prefix = 17; // 8 + 8 + 1
constexpr std::size_t subscribe_request_min_size     = 21; // + 2 + 2
constexpr std::size_t unsubscribe_request_size       = 8;
constexpr std::size_t unsubscribe_response_size      = 9; // 8 + 1

// Per-decoder policy bounds on the two uint16_t-prefixed string fields of
// subscribe_request. Both are attacker-controlled across an unauthenticated
// transport, so without an inline cap the 16-bit length prefix would let a
// peer force a 65 KB std::string allocation per subscribe frame. The bound
// here is structurally weaker than the upstream frame_reassembler cap and
// the wire::read_length_prefixed buffer-bounds check; it is the per-callsite
// policy lid.
constexpr std::size_t k_max_fqn       = 1024;
constexpr std::size_t k_max_type_name = 512;

// The trailing, flag-gated subscriber-QoS region. It rides one bounds-safe
// read_length_prefixed<uint16_t> guard, then a fixed-width layout (no nested length
// games). Present iff subscribe_request::has_qos; absent => the qos defaults, so a
// region-clear frame is byte-identical to the pre-region encoding.
//
//   durability            u8  @ 0   {0=none, 1=latest, 2=all}
//   delivery_mode         u8  @ 1   {0=push, 1=pull}
//   replay_depth          u32 @ 2   (0 => use the ring depth)
//   requested_flags       u8  @ 6   bit0=requires_source_identity,
//   bit1=requested_reliability_reliable requested_deadline_ns u64 @ 7   (0 = unset)
//   requested_lease_ns    u64 @ 15  (0 = unset)
//   requested_priority    u8  @ 23  (carry-only)
//   requested_max_message_bytes u32 @ 24 (0 = unset = always compatible)
//   reserved              u8[2] @ 28 (=0; future wire needs ride here)
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
static_assert(k_qos_region_size == 1 + 1 + 4 + 1 + 8 + 8 + 1 + 4 + 2,
              "the subscriber-QoS region must equal the sum of its fixed fields");
static_assert(k_qos_region_size == 30);

// The per-callsite policy lid on the attacker-controlled region length, mirroring
// k_max_fqn / k_max_type_name: a generous cap above the fixed 30-byte region so a
// claimed-huge uint16_t prefix is refused before any read.
constexpr std::size_t k_max_qos_region = 64;

// The QoS request-flag bit allocation inside requested_flags.
constexpr std::uint8_t k_qos_flag_requires_source_identity = 0x01;
constexpr std::uint8_t k_qos_flag_requested_reliable       = 0x02;
// The subscriber's strict/permissive RxO choice rides the next free reserved bit;
// clear = permissive (the friendly default), set = strict.
constexpr std::uint8_t k_qos_flag_rxo_strict = 0x04;
// The subscriber's strict TYPED attach posture rides the next free reserved bit;
// clear = lenient (the friendly default, attaches to an untyped producer), set =
// strict (refuses an untyped producer with subscribe_status::type_undeclared).
constexpr std::uint8_t k_qos_flag_typed_strict = 0x08;

}

}

// The encode/decode codecs over the shapes above are relocated to detail/subscribe_codec.h; the
// include keeps every wire::encode_subscribe_* / decode_subscribe_* call site resolving unchanged.
#include "plexus/wire/detail/subscribe_codec.h"

#endif
