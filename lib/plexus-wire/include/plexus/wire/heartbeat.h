#ifndef HPP_GUARD_PLEXUS_WIRE_HEARTBEAT_H
#define HPP_GUARD_PLEXUS_WIRE_HEARTBEAT_H

#include "plexus/wire/byte_order.h"
#include "plexus/wire/frame.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// The session-level presence assert (the keepalive/heartbeat spine). The asserting
// peer's identity already rides the frame_header (the session_id minted at the
// handshake and the pinned receive-path peer), so the payload is minimal: a version
// byte the receiver may branch on for a future heartbeat shape, plus one reserved
// byte held at 0 for forward extension. A heartbeat carries no per-topic state — it
// refreshes a per-endpoint presence clock, not a per-topic deadline clock.
struct heartbeat
{
    std::uint8_t version  = 1;
    std::uint8_t reserved = 0;

    friend bool operator==(const heartbeat &, const heartbeat &) = default;
};

namespace detail {

// Fixed-width payload layout (no length prefix, no allocation by length):
//   version  u8 @ 0
//   reserved u8 @ 1
// Byte-sum = 2 bytes, fixed.
constexpr std::size_t heartbeat_payload_size = 2;

}

constexpr std::size_t k_heartbeat_payload_size = detail::heartbeat_payload_size;

// Encode the fixed-width heartbeat payload into a reused scratch buffer (the
// allocation-light control-emit path the session uses). resize() reuses the buffer's
// grown capacity so a steady-state emit does not allocate.
inline void encode_heartbeat_into(std::vector<std::byte> &out, const heartbeat &hb)
{
    out.resize(detail::heartbeat_payload_size);
    wire::detail::write_u8(out.data(), hb.version);
    wire::detail::write_u8(out.data() + 1, hb.reserved);
}

inline std::vector<std::byte> encode_heartbeat(const heartbeat &hb)
{
    std::vector<std::byte> buf;
    encode_heartbeat_into(buf, hb);
    return buf;
}

// Decode a heartbeat from an untrusted payload. The size guard is the bounds-safe
// gate: a buffer shorter than the fixed width is rejected to nullopt BEFORE any read
// (fixed-width, no length prefix => no over-read, no allocation by length). An
// over-long buffer decodes the fixed prefix and ignores the trailing bytes
// (forward-compatible — a later heartbeat shape may append fields a v1 peer skips),
// mirroring the fixed-field decode discipline of fetch_latched.
inline std::optional<heartbeat> decode_heartbeat(std::span<const std::byte> payload)
{
    if(payload.size() < detail::heartbeat_payload_size)
        return std::nullopt;

    return heartbeat{.version = wire::detail::read_u8(payload.data()), .reserved = wire::detail::read_u8(payload.data() + 1)};
}

}

#endif
