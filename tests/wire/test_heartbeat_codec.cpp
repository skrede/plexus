// The heartbeat codec oracle: the new untrusted-input decoder pinned ahead of the
// milestone-boundary fuzz. It proves the fixed-width encode/decode round-trip, the
// bounds-safe short-buffer rejection (a buffer one byte short decodes to nullopt,
// never an over-read), and the forward-compatible over-long handling (the fixed
// prefix decodes, the trailing bytes are ignored). A pure header-only wire unit.

#include "plexus/wire/heartbeat.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::wire::heartbeat;
using plexus::wire::encode_heartbeat;
using plexus::wire::decode_heartbeat;
using plexus::wire::k_heartbeat_payload_size;

TEST_CASE("heartbeat codec: encode then decode round-trips the fixed payload")
{
    const heartbeat hb{.version = 1, .reserved = 0};
    const auto      bytes = encode_heartbeat(hb);
    REQUIRE(bytes.size() == k_heartbeat_payload_size);

    const auto decoded = decode_heartbeat(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == hb);
}

TEST_CASE("heartbeat codec: a short buffer decodes to nullopt (no over-read)")
{
    // One byte short of the fixed width: the bounds-safe gate rejects before any read.
    std::vector<std::byte> shortbuf(k_heartbeat_payload_size - 1, std::byte{0});
    REQUIRE(!decode_heartbeat(shortbuf).has_value());

    // An empty buffer is likewise rejected.
    REQUIRE(!decode_heartbeat(std::span<const std::byte>{}).has_value());
}

TEST_CASE("heartbeat codec: an over-long buffer decodes the fixed prefix and ignores the rest")
{
    auto bytes = encode_heartbeat(heartbeat{.version = 1, .reserved = 0});
    // Append trailing bytes a future heartbeat shape might carry; a v1 decode skips them.
    bytes.push_back(std::byte{0xFF});
    bytes.push_back(std::byte{0xAB});

    const auto decoded = decode_heartbeat(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->version == 1);
    REQUIRE(decoded->reserved == 0);
}

TEST_CASE("heartbeat codec: the msg_type slot is additive (0x0C, no protocol bump)")
{
    // The heartbeat rides the next free msg_type after subscribe_response, filled
    // additively — a peer that never emits one is byte-identical to a pre-heartbeat peer.
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::heartbeat) == 0x0C);
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::subscribe_response) == 0x0B);
}
