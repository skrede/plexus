#ifndef HPP_GUARD_PLEXUS_STREAM_SERIAL_FRAME_H
#define HPP_GUARD_PLEXUS_STREAM_SERIAL_FRAME_H

#include "plexus/wire/crc32c.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::stream {

inline constexpr std::size_t crc_trailer_size = 4;

// The CRCs are written little-endian, while the header fields are big-endian.
inline std::array<std::byte, crc_trailer_size> crc_trailer(std::span<const std::byte> header, std::span<const std::byte> payload) noexcept
{
    const std::uint32_t crc = wire::crc32c(payload, wire::crc32c(header));
    return {std::byte(crc & 0xFFu), std::byte((crc >> 8) & 0xFFu), std::byte((crc >> 16) & 0xFFu), std::byte((crc >> 24) & 0xFFu)};
}

inline std::uint32_t read_crc_le(std::span<const std::byte> t) noexcept
{
    return std::to_integer<std::uint32_t>(t[0]) | (std::to_integer<std::uint32_t>(t[1]) << 8) | (std::to_integer<std::uint32_t>(t[2]) << 16) |
            (std::to_integer<std::uint32_t>(t[3]) << 24);
}

}

#endif
