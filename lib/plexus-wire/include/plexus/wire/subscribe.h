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
    subscribed                   = 0x01,
    created                      = 0x02,
    type_mismatch                = 0x03,
    already_subscribed           = 0x04,
    // A soft request-vs-offered field failed under a strict subscriber.
    incompatible_qos             = 0x05,
    // The always-hard requires_source_identity field was unmet (any mode).
    source_identity_incompatible = 0x06,
    // A permissive accept whose trailing degraded_flags byte names the soft fields
    // that went unsatisfied.
    subscribed_degraded          = 0x07,
    // A strict-posture subscriber attached to a producer that declared no type. Only
    // ever emitted to a subscriber that requested the strict attach posture, so an old
    // peer (which never sets that bit) never receives it (append-safe).
    type_undeclared              = 0x08
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
    uint8_t  durability;        // {0=none, 1=latest, 2=all}
    uint8_t  delivery_mode;     // {0=push, 1=pull}
    uint32_t replay_depth;      // 0 => use the ring depth
    uint8_t  requested_flags;   // bit0=requires_source_identity, bit1=requested_reliability_reliable
    uint64_t requested_deadline_ns; // 0 = unset
    uint64_t requested_lease_ns;    // 0 = unset
    uint8_t  requested_priority;    // carry-only; default band 0
};

struct subscribe_request
{
    std::string fqn;
    std::string type_name;
    uint64_t topic_hash;
    uint64_t type_hash;
    endpoint_source_type source;
    // The flag-gated, trailing QoS region. has_qos=false (the default) encodes
    // NO trailing bytes, so the frame is byte-identical to the pre-region layout
    // a v3 producer would write; the decoder maps an absent region to the qos
    // defaults. has_qos=true appends the fixed 26-byte region after type_name.
    bool has_qos = false;
    subscribe_qos_region qos{};
};

struct subscribe_response
{
    uint64_t topic_hash;
    subscribe_status status;
    // The optional trailing degraded-field surface (gated like subscribe_request's
    // has_qos region). has_degraded=false (the default) encodes NO trailing byte, so
    // every refusal and every clean `subscribed` stays byte-identical to the pre-extension
    // 9-byte layout a v4 peer wrote. has_degraded=true (a permissive subscribed_degraded
    // accept) appends one byte carrying the unsatisfied soft-field bitmask.
    bool has_degraded = false;
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
    uint64_t topic_hash;
    unsubscribe_status status;
};

namespace detail {

// Subscribe request wire layout (the slice carries no service-policy fields):
//   topic_hash(8) + type_hash(8) + endpoint_source_type(1)
//   + fqn_len(2) + fqn_bytes + type_name_len(2) + type_name_bytes
// Minimum = 8 + 8 + 1 + 2 + 2 = 21 bytes

constexpr std::size_t subscribe_request_fixed_prefix = 17; // 8 + 8 + 1
constexpr std::size_t subscribe_request_min_size = 21;     // + 2 + 2
constexpr std::size_t unsubscribe_request_size = 8;
constexpr std::size_t unsubscribe_response_size = 9; // 8 + 1

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
//   requested_flags       u8  @ 6   bit0=requires_source_identity, bit1=requested_reliability_reliable
//   requested_deadline_ns u64 @ 7   (0 = unset)
//   requested_lease_ns    u64 @ 15  (0 = unset)
//   requested_priority    u8  @ 23  (carry-only)
//   reserved              u8[2] @ 24 (=0; future wire needs ride here)
// Byte-sum = 1+1+4+1+8+8+1+2 = 26 bytes, fixed.
constexpr std::size_t k_qos_durability_off    = 0;
constexpr std::size_t k_qos_delivery_off      = 1;
constexpr std::size_t k_qos_replay_depth_off  = 2;
constexpr std::size_t k_qos_flags_off         = 6;
constexpr std::size_t k_qos_deadline_ns_off   = 7;
constexpr std::size_t k_qos_lease_ns_off       = 15;
constexpr std::size_t k_qos_priority_off       = 23;
constexpr std::size_t k_qos_reserved_off       = 24;
constexpr std::size_t k_qos_reserved_size      = 2;

constexpr std::size_t k_qos_region_size = k_qos_reserved_off + k_qos_reserved_size;
static_assert(k_qos_region_size == 1 + 1 + 4 + 1 + 8 + 8 + 1 + 2,
              "the subscriber-QoS region must equal the sum of its fixed fields");
static_assert(k_qos_region_size == 26);

// The per-callsite policy lid on the attacker-controlled region length, mirroring
// k_max_fqn / k_max_type_name: a generous cap above the fixed 26-byte region so a
// claimed-huge uint16_t prefix is refused before any read.
constexpr std::size_t k_max_qos_region = 64;

// The QoS request-flag bit allocation inside requested_flags.
constexpr std::uint8_t k_qos_flag_requires_source_identity = 0x01;
constexpr std::uint8_t k_qos_flag_requested_reliable       = 0x02;
// The subscriber's strict/permissive RxO choice rides the next free reserved bit;
// clear = permissive (the friendly default), set = strict.
constexpr std::uint8_t k_qos_flag_rxo_strict               = 0x04;
// The subscriber's strict TYPED attach posture rides the next free reserved bit;
// clear = lenient (the friendly default, attaches to an untyped producer), set =
// strict (refuses an untyped producer with subscribe_status::type_undeclared).
constexpr std::uint8_t k_qos_flag_typed_strict             = 0x08;

}

// Write the fixed 26-byte QoS region at p (caller guarantees 26 writable bytes).
inline void write_qos_region(std::byte *p, const subscribe_qos_region &qos)
{
    wire::detail::write_u8(p + detail::k_qos_durability_off, qos.durability);
    wire::detail::write_u8(p + detail::k_qos_delivery_off, qos.delivery_mode);
    wire::detail::write_u32(p + detail::k_qos_replay_depth_off, qos.replay_depth);
    wire::detail::write_u8(p + detail::k_qos_flags_off, qos.requested_flags);
    wire::detail::write_u64(p + detail::k_qos_deadline_ns_off, qos.requested_deadline_ns);
    wire::detail::write_u64(p + detail::k_qos_lease_ns_off, qos.requested_lease_ns);
    wire::detail::write_u8(p + detail::k_qos_priority_off, qos.requested_priority);
    wire::detail::write_u8(p + detail::k_qos_reserved_off, 0);
    wire::detail::write_u8(p + detail::k_qos_reserved_off + 1, 0);
}

inline std::vector<std::byte> encode_subscribe_request(const subscribe_request &req)
{
    auto total = detail::subscribe_request_fixed_prefix + 2 + req.fqn.size() + 2 + req.type_name.size();
    if(req.has_qos)
        total += 2 + detail::k_qos_region_size;   // uint16_t length prefix + the fixed region
    std::vector<std::byte> buf(total);
    auto *p = buf.data();

    wire::detail::write_u64(p, req.topic_hash);
    p += 8;
    wire::detail::write_u64(p, req.type_hash);
    p += 8;
    wire::detail::write_u8(p, static_cast<uint8_t>(req.source));
    p += 1;

    wire::detail::write_u16(p, static_cast<uint16_t>(req.fqn.size()));
    p += 2;
    if(!req.fqn.empty())
    {
        std::memcpy(p, req.fqn.data(), req.fqn.size());
        p += req.fqn.size();
    }

    wire::detail::write_u16(p, static_cast<uint16_t>(req.type_name.size()));
    p += 2;
    if(!req.type_name.empty())
    {
        std::memcpy(p, req.type_name.data(), req.type_name.size());
        p += req.type_name.size();
    }

    if(req.has_qos)
    {
        wire::detail::write_u16(p, static_cast<uint16_t>(detail::k_qos_region_size));
        p += 2;
        write_qos_region(p, req.qos);
    }

    return buf;
}

inline std::optional<subscribe_request> decode_subscribe_request(std::span<const std::byte> payload)
{
    if(payload.size() < detail::subscribe_request_min_size)
        return std::nullopt;

    subscribe_request req{};
    auto *p = payload.data();

    req.topic_hash = wire::detail::read_u64(p);
    p += 8;
    req.type_hash = wire::detail::read_u64(p);
    p += 8;
    req.source = static_cast<endpoint_source_type>(wire::detail::read_u8(p));
    p += 1;

    std::size_t consumed = detail::subscribe_request_fixed_prefix;

    // fqn_len(2) + fqn + type_name_len(2) + type_name
    auto fqn_span = read_length_prefixed<uint16_t>(payload, consumed);
    if(!fqn_span)
        return std::nullopt;
    if(fqn_span->size() > detail::k_max_fqn)
        return std::nullopt;
    req.fqn.assign(reinterpret_cast<const char*>(fqn_span->data()), fqn_span->size());

    auto type_name_span = read_length_prefixed<uint16_t>(payload, consumed);
    if(!type_name_span)
        return std::nullopt;
    if(type_name_span->size() > detail::k_max_type_name)
        return std::nullopt;
    req.type_name.assign(reinterpret_cast<const char*>(type_name_span->data()), type_name_span->size());

    // The trailing, flag-gated QoS region (a NEW untrusted-input surface). No
    // trailing bytes => the region is absent and req.qos stays defaulted (a v3
    // producer never wrote it). A present region must match the fixed size
    // EXACTLY and stay within the per-callsite cap, else nullopt — never a
    // mis-parse, never an over-read (read_length_prefixed advances consumed on
    // success only).
    if(consumed < payload.size())
    {
        auto region = read_length_prefixed<uint16_t>(payload, consumed);
        if(!region)
            return std::nullopt;
        if(region->size() != detail::k_qos_region_size
           || region->size() > detail::k_max_qos_region)
            return std::nullopt;
        const auto *q = region->data();
        req.qos.durability            = wire::detail::read_u8(q + detail::k_qos_durability_off);
        req.qos.delivery_mode         = wire::detail::read_u8(q + detail::k_qos_delivery_off);
        req.qos.replay_depth          = wire::detail::read_u32(q + detail::k_qos_replay_depth_off);
        req.qos.requested_flags       = wire::detail::read_u8(q + detail::k_qos_flags_off);
        req.qos.requested_deadline_ns = wire::detail::read_u64(q + detail::k_qos_deadline_ns_off);
        req.qos.requested_lease_ns    = wire::detail::read_u64(q + detail::k_qos_lease_ns_off);
        req.qos.requested_priority    = wire::detail::read_u8(q + detail::k_qos_priority_off);
        req.has_qos = true;
    }
    return req;
}

inline std::vector<std::byte> encode_subscribe_response(const subscribe_response &resp)
{
    // A permissive degraded-accept appends the trailing degraded_flags byte; every
    // other response is the byte-identical 9-byte layout.
    std::vector<std::byte> buf(resp.has_degraded ? subscribe_response_size + 1 : subscribe_response_size);
    wire::detail::write_u64(buf.data(), resp.topic_hash);
    wire::detail::write_u8(buf.data() + 8, static_cast<uint8_t>(resp.status));
    if(resp.has_degraded)
        wire::detail::write_u8(buf.data() + 9, resp.degraded_flags);
    return buf;
}

inline std::optional<subscribe_response> decode_subscribe_response(std::span<const std::byte> payload)
{
    if(payload.size() < subscribe_response_size)
        return std::nullopt;

    subscribe_response resp{
            .topic_hash = wire::detail::read_u64(payload.data()),
            .status     = static_cast<subscribe_status>(wire::detail::read_u8(payload.data() + 8))
    };
    // The optional trailing degraded byte: present iff the response carried more than
    // the 9-byte floor (a permissive degraded-accept). A bare 9-byte response from a
    // v4 peer maps to "no degradation" — fully back-compatible.
    if(payload.size() > subscribe_response_size)
    {
        resp.has_degraded   = true;
        resp.degraded_flags = wire::detail::read_u8(payload.data() + 9);
    }
    return resp;
}

inline std::vector<std::byte> encode_unsubscribe_request(const unsubscribe_request &req)
{
    std::vector<std::byte> buf(detail::unsubscribe_request_size);
    wire::detail::write_u64(buf.data(), req.topic_hash);
    return buf;
}

inline std::optional<unsubscribe_request> decode_unsubscribe_request(std::span<const std::byte> payload)
{
    if(payload.size() < detail::unsubscribe_request_size)
        return std::nullopt;

    return unsubscribe_request{.topic_hash = wire::detail::read_u64(payload.data())};
}

inline std::vector<std::byte> encode_unsubscribe_response(const unsubscribe_response &resp)
{
    std::vector<std::byte> buf(detail::unsubscribe_response_size);
    wire::detail::write_u64(buf.data(), resp.topic_hash);
    wire::detail::write_u8(buf.data() + 8, static_cast<uint8_t>(resp.status));
    return buf;
}

inline std::optional<unsubscribe_response> decode_unsubscribe_response(std::span<const std::byte> payload)
{
    if(payload.size() < detail::unsubscribe_response_size)
        return std::nullopt;

    return unsubscribe_response{
            .topic_hash = wire::detail::read_u64(payload.data()),
            .status     = static_cast<unsubscribe_status>(wire::detail::read_u8(payload.data() + 8))
    };
}

}

#endif
