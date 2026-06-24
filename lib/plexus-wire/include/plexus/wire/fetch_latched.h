#ifndef HPP_GUARD_PLEXUS_WIRE_FETCH_LATCHED_H
#define HPP_GUARD_PLEXUS_WIRE_FETCH_LATCHED_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/frame.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// The consumer-paced PULL request for a latched topic's retained history: a
// subscriber that chose delivery=pull asks for the most-recent max_samples retained
// frames on its own schedule rather than at the producer's pace. The reply is the
// data frames themselves (no reply control frame). max_samples is an attacker-
// controlled count over an unauthenticated transport, so the server caps the reply
// at min(max_samples, ring.count(), k_fetch_cap) — a huge or wrapping value can
// never force more than the retained count of owned ring frames.
struct fetch_latched_request
{
    uint64_t topic_hash;
    uint32_t max_samples;
};

namespace detail {

// Fixed-width request layout (no length prefix, no allocation):
//   topic_hash  u64 @ 0
//   max_samples u32 @ 8
// Byte-sum = 8 + 4 = 12 bytes, fixed.
constexpr std::size_t fetch_latched_request_size = 12;

}

inline std::vector<std::byte> encode_fetch_latched_request(const fetch_latched_request &req)
{
    std::vector<std::byte> buf(detail::fetch_latched_request_size);
    wire::detail::write_u64(buf.data(), req.topic_hash);
    wire::detail::write_u32(buf.data() + 8, req.max_samples);
    return buf;
}

// Decode a fetch_latched_request from an untrusted payload. The size guard is the
// bounds-safe gate: a payload shorter than the fixed 12 bytes is rejected to nullopt
// before any read (fixed-width, no length prefix => no over-read, no allocation).
inline std::optional<fetch_latched_request> decode_fetch_latched_request(std::span<const std::byte> payload)
{
    if(payload.size() < detail::fetch_latched_request_size)
        return std::nullopt;

    return fetch_latched_request{.topic_hash = wire::detail::read_u64(payload.data()), .max_samples = wire::detail::read_u32(payload.data() + 8)};
}

}

#endif
