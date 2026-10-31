#ifndef HPP_GUARD_PLEXUS_STREAM_SERIAL_FRAME_H
#define HPP_GUARD_PLEXUS_STREAM_SERIAL_FRAME_H

#include "plexus/wire/frame.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/reader.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace plexus::stream {

// The serial format's identity, written where the wire frame carries its magic; the deframer accepts only this value.
inline constexpr std::array<std::byte, 2> k_serial_format_id{std::byte{0x53}, std::byte{0x50}};

inline constexpr std::size_t crc_trailer_size   = 4;
inline constexpr std::size_t serial_header_size = wire::header_size + sizeof(std::uint32_t);

enum class serial_format : std::uint8_t
{
    crc32c_header
};

inline std::optional<serial_format> peek_serial_format(std::span<const std::byte> bytes) noexcept
{
    if(bytes.size() < k_serial_format_id.size() || !std::equal(k_serial_format_id.begin(), k_serial_format_id.end(), bytes.begin()))
        return std::nullopt;
    return serial_format::crc32c_header;
}

// The CRCs are written little-endian, while the header fields are big-endian.
inline void write_crc_le(std::span<std::byte> out, std::uint32_t crc) noexcept
{
    for(std::size_t i = 0; i < sizeof(crc); ++i)
        out[i] = std::byte((crc >> (8 * i)) & 0xFFu);
}

inline std::uint32_t read_crc_le(std::span<const std::byte> t) noexcept
{
    return std::to_integer<std::uint32_t>(t[0]) | (std::to_integer<std::uint32_t>(t[1]) << 8) | (std::to_integer<std::uint32_t>(t[2]) << 16) |
            (std::to_integer<std::uint32_t>(t[3]) << 24);
}

inline std::array<std::byte, crc_trailer_size> crc_trailer(std::span<const std::byte> header, std::span<const std::byte> payload) noexcept
{
    std::array<std::byte, crc_trailer_size> trailer{};
    write_crc_le(trailer, wire::crc32c(payload, wire::crc32c(header)));
    return trailer;
}

inline std::array<std::byte, serial_header_size> encode_serial_header(std::span<const std::byte> wire_header) noexcept
{
    std::array<std::byte, serial_header_size> header{};
    const auto fields = std::copy(k_serial_format_id.begin(), k_serial_format_id.end(), header.begin());
    std::copy(wire_header.begin() + k_serial_format_id.size(), wire_header.begin() + wire::header_size, fields);
    const std::span<std::byte> out{header};
    write_crc_le(out.subspan(wire::header_size), wire::crc32c(out.first(wire::header_size)));
    return header;
}

inline bool serial_header_intact(std::span<const std::byte> header) noexcept
{
    return read_crc_le(header.subspan(wire::header_size)) == wire::crc32c(header.first(wire::header_size));
}

inline std::uint64_t serial_payload_len(std::span<const std::byte> header) noexcept
{
    wire::reader r{header.subspan(wire::header_size - sizeof(std::uint64_t))};
    return r.u64();
}

// Rewrites the serial header in place as the wire header that ends where the payload starts, so a verified frame is
// handed on without a copy.
inline std::span<const std::byte> wire_frame_in_place(std::span<std::byte> serial_frame) noexcept
{
    constexpr std::size_t shift = serial_header_size - wire::header_size;
    const auto fields           = serial_frame.begin() + k_serial_format_id.size();
    std::copy_backward(fields, serial_frame.begin() + wire::header_size, serial_frame.begin() + serial_header_size);
    serial_frame[shift]     = wire::magic_byte_0;
    serial_frame[shift + 1] = wire::magic_byte_1;
    return serial_frame.subspan(shift);
}

}

#endif
