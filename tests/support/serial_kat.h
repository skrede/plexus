#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_SERIAL_KAT_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_SERIAL_KAT_H

#include "plexus/wire/frame.h"

#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <stdexcept>
#include <string_view>

namespace plexus::testing {

struct serial_kat_vector
{
    std::string_view name;
    std::array<std::byte, 2> format_id;
    wire::frame_header header;
    std::string_view payload_hex;
    std::string_view frame_hex;
};

inline std::uint8_t hex_nibble(char c) noexcept
{
    return static_cast<std::uint8_t>(c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10);
}

inline std::vector<std::byte> from_hex(std::string_view hex)
{
    std::vector<std::byte> out;
    for(std::size_t i = 0; i + 1 < hex.size(); ++i)
    {
        if(hex[i] == ' ')
            continue;
        out.push_back(std::byte(hex_nibble(hex[i]) << 4 | hex_nibble(hex[i + 1])));
        ++i;
    }
    return out;
}

// The expected bytes are literals captured once from the codec and never recomputed by the routine under test.
inline constexpr std::array serial_kat_vectors{
        serial_kat_vector{
                .name        = "short ascii payload",
                .format_id   = {std::byte{0x53}, std::byte{0x50}},
                .header      = {.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 19},
                .payload_hex = "706c657875732d73657269616c2d6672616d65",
                .frame_hex   = "5350 01 00 0000000000000001 0000000000000000 0000000000000013 7f0d614e 706c657875732d73657269616c2d6672616d65 69eea661",
        },
        serial_kat_vector{
                .name        = "empty payload",
                .format_id   = {std::byte{0x53}, std::byte{0x50}},
                .header      = {.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                .payload_hex = "",
                .frame_hex   = "5350 01 00 0000000000000000 0000000000000000 0000000000000000 bbe58b12 c74b6748",
        },
        serial_kat_vector{
                .name        = "all-ones header fields",
                .format_id   = {std::byte{0x53}, std::byte{0x50}},
                .header      = {.type = wire::msg_type::unidirectional, .flags = 0xFF, .session_id = 0xFFFFFFFFFFFFFFFF, .timestamp_ns = 0xFFFFFFFFFFFFFFFF, .payload_len = 2},
                .payload_hex = "a55a",
                .frame_hex   = "5350 01 ff ffffffffffffffff ffffffffffffffff 0000000000000002 daf848d1 a55a 316e8db4",
        },
        serial_kat_vector{
                .name        = "anchor bytes inside the payload",
                .format_id   = {std::byte{0x53}, std::byte{0x50}},
                .header      = {.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 4},
                .payload_hex = "53505650",
                .frame_hex   = "5350 01 00 0000000000000001 0000000000000000 0000000000000004 fbaef58a 53505650 5e9437e3",
        },
        serial_kat_vector{
                .name        = "two-byte payload",
                .format_id   = {std::byte{0x53}, std::byte{0x50}},
                .header      = {.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 2},
                .payload_hex = "1122",
                .frame_hex   = "5350 01 00 0000000000000000 0000000000000000 0000000000000002 4c95b0f3 1122 55ebcbba",
        },
        serial_kat_vector{
                .name        = "rpc request with distinct field bytes",
                .format_id   = {std::byte{0x53}, std::byte{0x50}},
                .header      = {.type = wire::msg_type::rpc_request, .flags = 0x01, .session_id = 0x0102030405060708, .timestamp_ns = 0x1122334455667788, .payload_len = 4},
                .payload_hex = "70696e67",
                .frame_hex   = "5350 09 01 0102030405060708 1122334455667788 0000000000000004 78cce468 70696e67 a8dfb563",
        },
};

inline const serial_kat_vector &serial_kat_row(std::string_view name)
{
    const auto it = std::ranges::find(serial_kat_vectors, name, &serial_kat_vector::name);
    if(it == serial_kat_vectors.end())
        throw std::out_of_range{std::string{name}};
    return *it;
}

}

#endif
