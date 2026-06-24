#pragma once

#include "plexus/io/frame_router.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace frame_router_fixture {

using plexus::io::frame_router;
namespace wire = plexus::wire;

// A test logger whose warn() bumps a counter — proves the warn-and-drop seam fires.
struct counting_logger final : plexus::log::logger
{
    void warn(std::string_view) override
    {
        ++count;
    }
    std::size_t count{0};
};

// Builds a complete (header-on) frame of the given type carrying body as the
// inner payload — exactly the shape both backends post to on_data.
inline std::vector<std::byte> make_frame(wire::msg_type type, std::string_view body)
{
    std::vector<std::byte> inner(body.size());
    for(std::size_t i = 0; i < body.size(); ++i)
        inner[i] = static_cast<std::byte>(static_cast<unsigned char>(body[i]));

    wire::frame_header hdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = inner.size()};
    return wire::encode_frame(hdr, inner);
}

inline std::string to_string(std::span<const std::byte> s)
{
    return std::string(reinterpret_cast<const char *>(s.data()), s.size());
}

} // namespace frame_router_fixture
