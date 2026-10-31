#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_SERIAL_BYTES_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_SERIAL_BYTES_H

#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"

#include <span>
#include <vector>
#include <cstddef>

namespace plexus::testing {

inline constexpr std::size_t k_serial_header_bytes     = stream::serial_header_size;
inline constexpr std::size_t k_serial_timestamp_offset = 12;

inline std::vector<std::byte> serial_bytes(std::span<const std::byte> framed)
{
    const auto header  = stream::encode_serial_header(framed.first(wire::header_size));
    const auto payload = framed.subspan(wire::header_size);
    const auto trailer = stream::crc_trailer(header, payload);
    std::vector<std::byte> out{header.begin(), header.end()};
    out.insert(out.end(), payload.begin(), payload.end());
    out.insert(out.end(), trailer.begin(), trailer.end());
    return out;
}

}

#endif
