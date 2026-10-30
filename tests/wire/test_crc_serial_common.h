#ifndef HPP_GUARD_TESTS_WIRE_CRC_SERIAL_COMMON_H
#define HPP_GUARD_TESTS_WIRE_CRC_SERIAL_COMMON_H

#include "support/serial_bytes.h"

#include "plexus/stream/crc_serial.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

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
    const auto pl = as_bytes(payload);
    return plexus::testing::serial_bytes(encode_frame(make_header(pl.size()), pl));
}

inline std::string header_on(std::string_view payload)
{
    const auto f = framed(payload);
    return std::string{reinterpret_cast<const char *>(f.data()), header_size + payload.size()};
}

struct sink
{
    std::vector<std::string> emitted;
    std::int32_t drops{0};

    crc_serial_inbound make()
    {
        crc_serial_inbound dec;
        dec.on_match([this](std::span<const std::byte> f) { emitted.emplace_back(reinterpret_cast<const char *>(f.data()), f.size()); });
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
