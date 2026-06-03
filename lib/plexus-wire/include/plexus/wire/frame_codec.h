#ifndef HPP_GUARD_PLEXUS_WIRE_FRAME_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_FRAME_CODEC_H

#include "plexus/wire/byte_order.h"
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
    auto *p = buf.data();

    p[0] = magic_byte_0;
    p[1] = magic_byte_1;
    detail::write_u8(p + 2, static_cast<uint8_t>(hdr.type));
    detail::write_u8(p + 3, hdr.flags);
    detail::write_u8(p + 4, hdr.session_id);
    detail::write_u64(p + 5, hdr.timestamp_ns);
    detail::write_u64(p + 13, hdr.payload_len);

    return buf;
}

inline std::optional<frame_header> decode_header(std::span<const std::byte> data)
{
    if(data.size() < header_size)
        return std::nullopt;

    if(data[0] != magic_byte_0 || data[1] != magic_byte_1)
        return std::nullopt;

    auto *p = data.data();
    return frame_header{
            .type         = static_cast<msg_type>(detail::read_u8(p + 2)),
            .flags        = detail::read_u8(p + 3),
            .session_id   = detail::read_u8(p + 4),
            .timestamp_ns = detail::read_u64(p + 5),
            .payload_len  = detail::read_u64(p + 13)
    };
}

inline std::vector<std::byte> encode_frame(const frame_header &hdr, std::span<const std::byte> payload)
{
    auto adjusted = hdr;
    adjusted.payload_len = payload.size();

    auto header_bytes = encode_header(adjusted);

    std::vector<std::byte> frame;
    frame.reserve(header_size + payload.size());
    frame.insert(frame.end(), header_bytes.begin(), header_bytes.end());
    frame.insert(frame.end(), payload.begin(), payload.end());

    return frame;
}

inline uint64_t now_timestamp_ns()
{
    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
    return static_cast<uint64_t>(ns.count());
}

}

#endif
