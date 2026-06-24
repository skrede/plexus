#ifndef HPP_GUARD_PLEXUS_TESTS_WIRE_TEST_FRAME_REASSEMBLER_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_WIRE_TEST_FRAME_REASSEMBLER_COMMON_H

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame_reassembler.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <span>
#include <vector>

using namespace plexus::wire;

inline std::vector<std::byte> make_frame(msg_type type, uint8_t flags, uint64_t ts, std::span<const std::byte> payload)
{
    frame_header hdr{.type = type, .flags = flags, .session_id = 0, .timestamp_ns = ts, .payload_len = payload.size()};
    return encode_frame(hdr, payload);
}

#endif
