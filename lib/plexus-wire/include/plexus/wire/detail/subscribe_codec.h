#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_SUBSCRIBE_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_SUBSCRIBE_CODEC_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/length_prefixed.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>

namespace plexus::wire {

// Write the fixed 30-byte QoS region at p (caller guarantees 30 writable bytes).
inline void write_qos_region(std::byte *p, const subscribe_qos_region &qos)
{
    wire::detail::write_u8(p + detail::k_qos_durability_off, qos.durability);
    wire::detail::write_u8(p + detail::k_qos_delivery_off, qos.delivery_mode);
    wire::detail::write_u32(p + detail::k_qos_replay_depth_off, qos.replay_depth);
    wire::detail::write_u8(p + detail::k_qos_flags_off, qos.requested_flags);
    wire::detail::write_u64(p + detail::k_qos_deadline_ns_off, qos.requested_deadline_ns);
    wire::detail::write_u64(p + detail::k_qos_lease_ns_off, qos.requested_lease_ns);
    wire::detail::write_u8(p + detail::k_qos_priority_off, qos.requested_priority);
    wire::detail::write_u32(p + detail::k_qos_max_message_off, qos.requested_max_message_bytes);
    wire::detail::write_u8(p + detail::k_qos_reserved_off, 0);
    wire::detail::write_u8(p + detail::k_qos_reserved_off + 1, 0);
}

namespace detail {

// Write the fixed prefix (topic_hash, type_hash, source) + the two uint16-prefixed string
// fields, advancing p. The trailing QoS region is appended by the caller when present.
inline std::byte *write_subscribe_head(std::byte *p, const subscribe_request &req)
{
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
    return p;
}

// Decode the trailing flag-gated QoS region into req (untrusted input): a present region must
// match the fixed size EXACTLY and stay within the per-callsite cap, else false (consumed is
// advanced on success only, so a malformed region never over-reads).
inline bool read_subscribe_qos(std::span<const std::byte> payload, std::size_t &consumed, subscribe_request &req)
{
    auto region = read_length_prefixed<uint16_t>(payload, consumed);
    if(!region)
        return false;
    if(region->size() != k_qos_region_size || region->size() > k_max_qos_region)
        return false;
    const auto *q                       = region->data();
    req.qos.durability                  = wire::detail::read_u8(q + k_qos_durability_off);
    req.qos.delivery_mode               = wire::detail::read_u8(q + k_qos_delivery_off);
    req.qos.replay_depth                = wire::detail::read_u32(q + k_qos_replay_depth_off);
    req.qos.requested_flags             = wire::detail::read_u8(q + k_qos_flags_off);
    req.qos.requested_deadline_ns       = wire::detail::read_u64(q + k_qos_deadline_ns_off);
    req.qos.requested_lease_ns          = wire::detail::read_u64(q + k_qos_lease_ns_off);
    req.qos.requested_priority          = wire::detail::read_u8(q + k_qos_priority_off);
    req.qos.requested_max_message_bytes = wire::detail::read_u32(q + k_qos_max_message_off);
    req.has_qos                         = true;
    return true;
}

}

inline std::vector<std::byte> encode_subscribe_request(const subscribe_request &req)
{
    auto total = detail::subscribe_request_fixed_prefix + 2 + req.fqn.size() + 2 + req.type_name.size();
    if(req.has_qos)
        total += 2 + detail::k_qos_region_size; // uint16_t length prefix + the fixed region
    std::vector<std::byte> buf(total);
    auto                  *p = detail::write_subscribe_head(buf.data(), req);
    if(req.has_qos)
    {
        wire::detail::write_u16(p, static_cast<uint16_t>(detail::k_qos_region_size));
        p += 2;
        write_qos_region(p, req.qos);
    }
    return buf;
}

// NOLINTNEXTLINE(readability-function-size)
inline std::optional<subscribe_request> decode_subscribe_request(std::span<const std::byte> payload)
{
    if(payload.size() < detail::subscribe_request_min_size)
        return std::nullopt;

    subscribe_request req{};
    auto             *p = payload.data();
    req.topic_hash      = wire::detail::read_u64(p);
    req.type_hash       = wire::detail::read_u64(p + 8);
    req.source          = static_cast<endpoint_source_type>(wire::detail::read_u8(p + 16));

    std::size_t consumed = detail::subscribe_request_fixed_prefix;
    auto        fqn_span = read_length_prefixed<uint16_t>(payload, consumed);
    if(!fqn_span || fqn_span->size() > detail::k_max_fqn)
        return std::nullopt;
    req.fqn.assign(reinterpret_cast<const char *>(fqn_span->data()), fqn_span->size());

    auto type_name_span = read_length_prefixed<uint16_t>(payload, consumed);
    if(!type_name_span || type_name_span->size() > detail::k_max_type_name)
        return std::nullopt;
    req.type_name.assign(reinterpret_cast<const char *>(type_name_span->data()), type_name_span->size());

    // No trailing bytes => the QoS region is absent and req.qos stays defaulted (a v3 producer
    // never wrote it). A present region is decoded under the bounds gate.
    if(consumed < payload.size() && !detail::read_subscribe_qos(payload, consumed, req))
        return std::nullopt;
    return req;
}

inline std::vector<std::byte> encode_subscribe_response(const subscribe_response &resp)
{
    // A permissive degraded-accept appends the trailing degraded_flags byte; every other
    // response is the byte-identical 9-byte layout.
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
    subscribe_response resp{.topic_hash = wire::detail::read_u64(payload.data()), .status = static_cast<subscribe_status>(wire::detail::read_u8(payload.data() + 8))};
    // The optional trailing degraded byte: present iff the response carried more than the 9-byte
    // floor. A bare 9-byte response from a v4 peer maps to "no degradation".
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
    return unsubscribe_response{.topic_hash = wire::detail::read_u64(payload.data()), .status = static_cast<unsubscribe_status>(wire::detail::read_u8(payload.data() + 8))};
}
}

#endif
