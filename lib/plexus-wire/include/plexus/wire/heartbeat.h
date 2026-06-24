#ifndef HPP_GUARD_PLEXUS_WIRE_HEARTBEAT_H
#define HPP_GUARD_PLEXUS_WIRE_HEARTBEAT_H

#include "plexus/wire/frame.h"
#include "plexus/wire/byte_order.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// The session-level presence assert. The peer's identity rides the frame_header, so the
// payload is minimal: a version byte plus one reserved byte held at 0 for forward extension.
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

// Encode the fixed-width payload into a reused scratch buffer (resize() reuses capacity, so a
// steady-state emit does not allocate).
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

// Decode from an untrusted payload. A buffer shorter than the fixed width is rejected before
// any read; an over-long buffer decodes the fixed prefix and ignores trailing bytes
// (forward-compatible — a later heartbeat shape may append fields a v1 peer skips).
inline std::optional<heartbeat> decode_heartbeat(std::span<const std::byte> payload)
{
    if(payload.size() < detail::heartbeat_payload_size)
        return std::nullopt;

    return heartbeat{.version = wire::detail::read_u8(payload.data()), .reserved = wire::detail::read_u8(payload.data() + 1)};
}

}

#endif
