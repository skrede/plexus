#include "seed_sink.h"

#include "support/serial_bytes.h"

#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <initializer_list>

namespace {

using namespace plexus;

struct frame_spec
{
    wire::msg_type type;
    std::uint64_t session_id;
    std::span<const std::byte> payload;
};

std::span<const std::byte> text(std::string_view s) noexcept
{
    return std::as_bytes(std::span<const char>{s.data(), s.size()});
}

const std::array<frame_spec, 3> k_frames{{
        {wire::msg_type::unidirectional, 0x0102030405060708, text("hello")},
        {wire::msg_type::heartbeat, 1, text("\x01\x02\x03")},
        {wire::msg_type::rpc_request, 0, text("")},
}};

constexpr std::uint8_t k_header_byte  = 5;
constexpr std::uint8_t k_payload_byte = stream::serial_header_size + 2;
constexpr std::uint8_t k_first_end    = stream::serial_header_size + 5 + stream::crc_trailer_size;
constexpr std::uint8_t k_second_mid   = k_first_end + 20;

void append_le(seed &out, std::uint64_t value, std::size_t width)
{
    for(std::size_t i = 0; i < width; ++i)
        out.push_back(std::byte{static_cast<std::uint8_t>(value >> (8 * i))});
}

void append_structured_frame(seed &out, const frame_spec &f)
{
    append(out, {static_cast<std::uint8_t>(f.type), 0, 8});
    append_le(out, f.session_id, 8);
    append(out, {0});
    append_le(out, f.payload.size(), 2);
    append(out, f.payload);
}

std::span<const frame_spec> first(std::size_t count) noexcept
{
    return std::span{k_frames}.first(count);
}

// Grammar: an even mode byte, frame count minus one, the frames, then the mutation and chunk bytes in tail.
seed structured(std::span<const frame_spec> frames, std::initializer_list<std::uint8_t> tail)
{
    seed out;
    append(out, {0, static_cast<std::uint8_t>(frames.size() - 1)});
    for(const auto &f : frames)
        append_structured_frame(out, f);
    append(out, tail);
    return out;
}

// Grammar: an odd mode byte, the chunk plan in prefix, then the serial stream.
seed raw(std::initializer_list<std::uint8_t> prefix, std::span<const frame_spec> frames)
{
    seed out;
    append(out, {1});
    append(out, prefix);
    for(const auto &f : frames)
    {
        const wire::frame_header header{.type = f.type, .flags = 0, .session_id = f.session_id, .timestamp_ns = 0, .payload_len = 0};
        append(out, testing::serial_bytes(wire::encode_frame(header, f.payload)));
    }
    return out;
}

}

seed_set crc_serial_seeds()
{
    return {
            structured(first(1), {0, 0}),
            structured(first(2), {0, 3, 0, 4, 31}),
            structured(first(3), {0, 0}),
            structured(first(2), {1, 0, k_header_byte, 0, 0}),
            structured(first(2), {1, 1, k_payload_byte, 0, 0x01, 0}),
            structured(first(2), {1, 2, k_second_mid, 0, 0}),
            structured(first(2), {1, 3, k_first_end, 0, 3, 0x53, 0x00, 0x50, 0}),
            structured(first(2), {1, 4, k_first_end, 0, 0}),
            raw({0}, first(3)),
            raw({2, 6, 17}, first(3)),
            raw({0, 0x53, 0x50, 0xff}, first(2)),
    };
}
