#ifndef HPP_GUARD_PLEXUS_WIRE_FRAME_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_FRAME_CODEC_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/frame.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace plexus::wire {

inline std::array<std::byte, header_size> encode_header(const frame_header &hdr)
{
    std::array<std::byte, header_size> buf{};
    writer                             w{std::span<std::byte>{buf}};

    w.u8(std::to_integer<std::uint8_t>(magic_byte_0));
    w.u8(std::to_integer<std::uint8_t>(magic_byte_1));
    w.u8(static_cast<uint8_t>(hdr.type));
    w.u8(hdr.flags);
    w.u64(hdr.session_id);
    w.u64(hdr.timestamp_ns);
    w.u64(hdr.payload_len);

    return buf;
}

inline std::optional<frame_header> decode_header(std::span<const std::byte> data)
{
    if(data.size() < header_size)
        return std::nullopt;

    if(data[0] != magic_byte_0 || data[1] != magic_byte_1)
        return std::nullopt;

    reader r{data};
    r.u8();
    r.u8();
    return frame_header{.type = static_cast<msg_type>(r.u8()), .flags = r.u8(), .session_id = r.u64(), .timestamp_ns = r.u64(), .payload_len = r.u64()};
}

inline std::vector<std::byte> encode_frame(const frame_header &hdr, std::span<const std::byte> payload)
{
    auto adjusted        = hdr;
    adjusted.payload_len = payload.size();

    auto header_bytes = encode_header(adjusted);

    std::vector<std::byte> frame;
    frame.reserve(header_size + payload.size());
    frame.insert(frame.end(), header_bytes.begin(), header_bytes.end());
    frame.insert(frame.end(), payload.begin(), payload.end());

    return frame;
}

// Encode a framed message into a caller-owned buffer reused across calls. Like
// encode_unidirectional_into, resize() reuses capacity so a steady-state loop
// framing into the same out vector allocates nothing after warm-up. The header
// is written in place ahead of the payload copy.
inline void encode_frame_into(std::vector<std::byte> &out, const frame_header &hdr, std::span<const std::byte> payload)
{
    auto adjusted        = hdr;
    adjusted.payload_len = payload.size();

    auto header_bytes = encode_header(adjusted);

    out.resize(header_size + payload.size());
    std::memcpy(out.data(), header_bytes.data(), header_size);
    if(!payload.empty())
        std::memcpy(out.data() + header_size, payload.data(), payload.size());
}

inline uint64_t now_timestamp_ns()
{
    auto now = std::chrono::system_clock::now();
    auto ns  = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    return static_cast<uint64_t>(ns.count());
}

}

#endif
