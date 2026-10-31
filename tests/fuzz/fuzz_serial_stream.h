#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SERIAL_STREAM_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SERIAL_STREAM_H

#include "fuzz_input.h"

#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace fuzz_serial {

namespace wire   = plexus::wire;
namespace stream = plexus::stream;

using bytes      = std::vector<std::byte>;
using bytes_view = std::span<const std::byte>;

constexpr std::size_t k_max_payload = 256;

struct frame_extent
{
    std::size_t begin;
    std::size_t end;
    bool touched;
};

struct encoded_stream
{
    bytes serial;
    std::vector<bytes> frames;
    std::vector<frame_extent> extents;
    bool anchor_in_payload = false;
};

inline bool holds_format_id(bytes_view payload)
{
    const auto &id = stream::k_serial_format_id;
    return std::search(payload.begin(), payload.end(), id.begin(), id.end()) != payload.end();
}

inline void append_frame(fuzz_input &in, encoded_stream &s)
{
    wire::frame_header header{.type = static_cast<wire::msg_type>(in.u8()), .flags = in.u8(), .session_id = in.any(), .timestamp_ns = in.any(), .payload_len = 0};
    const auto payload       = std::as_bytes(in.take(static_cast<std::size_t>(in.uint(2)) % (k_max_payload + 1)));
    header.payload_len       = payload.size();
    const auto wire_header   = wire::encode_header(header);
    const auto serial_header = stream::encode_serial_header(wire_header);
    const auto trailer       = stream::crc_trailer(serial_header, payload);
    const std::size_t begin  = s.serial.size();
    s.serial.insert(s.serial.end(), serial_header.begin(), serial_header.end());
    s.serial.insert(s.serial.end(), payload.begin(), payload.end());
    s.serial.insert(s.serial.end(), trailer.begin(), trailer.end());
    s.extents.push_back({begin, s.serial.size(), false});
    bytes frame{wire_header.begin(), wire_header.end()};
    frame.insert(frame.end(), payload.begin(), payload.end());
    s.frames.push_back(std::move(frame));
    s.anchor_in_payload = s.anchor_in_payload || holds_format_id(payload);
}

// An insertion lands before byte at, so one at a frame's first byte leaves that frame's bytes intact.
inline void touch(std::vector<frame_extent> &extents, std::size_t at, bool insertion) noexcept
{
    for(auto &e : extents)
        e.touched = e.touched || ((insertion ? e.begin < at : e.begin <= at) && at < e.end);
}

inline void shift(std::vector<frame_extent> &extents, std::size_t at, std::size_t grown, std::size_t shrunk) noexcept
{
    for(auto &e : extents)
    {
        if(e.begin > at)
            e.begin = e.begin + grown - shrunk;
        if(e.end > at)
            e.end = e.end + grown - shrunk;
    }
}

inline void splice(encoded_stream &s, std::size_t at, bytes_view inserted)
{
    touch(s.extents, at, true);
    shift(s.extents, at, inserted.size(), 0);
    s.serial.insert(s.serial.begin() + static_cast<std::ptrdiff_t>(at), inserted.begin(), inserted.end());
}

inline void truncate(encoded_stream &s, std::size_t length)
{
    for(auto &e : s.extents)
    {
        e.touched = e.touched || e.end > length;
        e.begin   = std::min(e.begin, length);
        e.end     = std::min(e.end, length);
    }
    s.serial.resize(length);
}

inline void mutate(fuzz_input &in, encoded_stream &s)
{
    const std::uint8_t kind = in.u8() % 5;
    const std::size_t at    = static_cast<std::size_t>(in.uint(2)) % (s.serial.size() + 1);
    const bool inside       = at < s.serial.size();
    if(kind == 0 && inside)
    {
        touch(s.extents, at, false);
        shift(s.extents, at, 0, 1);
        s.serial.erase(s.serial.begin() + static_cast<std::ptrdiff_t>(at));
    }
    else if(kind == 1 && inside)
    {
        touch(s.extents, at, false);
        s.serial[at] ^= std::byte{static_cast<std::uint8_t>(1 + in.u8() % 255)};
    }
    else if(kind == 2)
        truncate(s, at);
    else if(kind == 3)
        splice(s, at, std::as_bytes(in.take(in.u8() % 32)));
    else if(kind == 4)
        splice(s, at, stream::k_serial_format_id);
}

}

#endif
