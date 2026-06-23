#ifndef HPP_GUARD_TESTS_WIRE_CRC_SERIAL_COMMON_H
#define HPP_GUARD_TESTS_WIRE_CRC_SERIAL_COMMON_H

#include "plexus/stream/crc_serial.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

// Shared fixtures for the CRC32C trailer + magic-resync decorator oracle: build a complete
// serial wire frame [header][payload][CRC32C trailer LE], and a sink that records the clean
// frames the decorator emits and counts the non-fatal crc_mismatch drops.

namespace crc_serial_test {

using namespace plexus::wire;
using namespace plexus::stream;

inline std::span<const std::byte> as_bytes(std::string_view s) noexcept
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline frame_header make_header(std::size_t payload_len)
{
    frame_header hdr{};
    hdr.type        = msg_type::unidirectional;
    hdr.flags       = 0;
    hdr.session_id  = 1;
    hdr.payload_len = payload_len;
    return hdr;
}

inline std::vector<std::byte> framed(std::string_view payload)
{
    const auto             pl    = as_bytes(payload);
    const auto             hdr   = encode_header(make_header(pl.size()));
    const auto             trail = crc_trailer(std::span<const std::byte>{hdr}, pl);
    std::vector<std::byte> out;
    out.insert(out.end(), hdr.begin(), hdr.end());
    out.insert(out.end(), pl.begin(), pl.end());
    out.insert(out.end(), trail.begin(), trail.end());
    return out;
}

inline std::string header_on(std::string_view payload)
{
    const auto f = framed(payload);
    return std::string{reinterpret_cast<const char *>(f.data()), header_size + payload.size()};
}

struct sink
{
    std::vector<std::string> emitted;
    int                      drops{0};

    crc_serial_inbound make()
    {
        crc_serial_inbound dec;
        dec.on_match([this](std::span<const std::byte> f)
                     { emitted.emplace_back(reinterpret_cast<const char *>(f.data()), f.size()); });
        dec.on_drop(
                [this](close_cause c)
                {
                    if(c == close_cause::crc_mismatch)
                        ++drops;
                });
        return dec;
    }
};

}

#endif
