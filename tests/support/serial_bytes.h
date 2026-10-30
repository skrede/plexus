#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_SERIAL_BYTES_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_SERIAL_BYTES_H

#include "plexus/stream/crc_serial.h"

#include "plexus/wire/frame.h"

#include <span>
#include <vector>
#include <cstddef>

namespace plexus::testing {

inline constexpr std::size_t k_serial_header_bytes     = wire::header_size;
inline constexpr std::size_t k_serial_timestamp_offset = 12;

inline std::vector<std::byte> serial_bytes(std::span<const std::byte> framed)
{
    const auto trailer = stream::crc_trailer(framed.first(wire::header_size), framed.subspan(wire::header_size));
    std::vector<std::byte> out{framed.begin(), framed.end()};
    out.insert(out.end(), trailer.begin(), trailer.end());
    return out;
}

}

#endif
