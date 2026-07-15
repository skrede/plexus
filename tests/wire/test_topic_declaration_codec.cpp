// The topic-declaration codec oracle: an untrusted-input decoder. It proves the three-state
// round-trip, the bounded type_name lid (an over-cap name rejects before the string is
// materialized), and the single-latch rejection of a truncated payload or an unknown state
// byte — nullopt, never a partial struct. A pure header-only wire unit.

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_declaration.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::wire::type_state;
using plexus::wire::topic_declaration;
using plexus::wire::encode_topic_declaration;
using plexus::wire::decode_topic_declaration;
using plexus::wire::detail::k_max_type_name;

TEST_CASE("topic_declaration codec: round-trips each of the three type states")
{
    topic_declaration td{.topic_hash = 0x0102030405060708ULL, .type_id = 0x1112131415161718ULL, .type_name = "", .state = type_state::undeclared};

    SECTION("undeclared carries no name")
    {
        td.state = type_state::undeclared;
    }
    SECTION("declared-empty carries an empty name")
    {
        td.state = type_state::declared_empty;
    }
    SECTION("declared carries the opaque name")
    {
        td.state     = type_state::declared;
        td.type_name = "geometry/Pose";
    }

    const auto decoded = decode_topic_declaration(encode_topic_declaration(td));

    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == td);
}

TEST_CASE("topic_declaration codec: a long type_name survives the round-trip")
{
    const topic_declaration td{.topic_hash = 1, .type_id = 2, .type_name = std::string(300, 'T'), .state = type_state::declared};

    const auto decoded = decode_topic_declaration(encode_topic_declaration(td));

    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == td);
}

TEST_CASE("topic_declaration codec: every truncation of a valid frame decodes to nullopt")
{
    const topic_declaration td{.topic_hash = 1, .type_id = 2, .type_name = "sensor/Imu", .state = type_state::declared};
    const auto bytes = encode_topic_declaration(td);

    for(std::size_t n = 0; n < bytes.size(); ++n)
        REQUIRE(!decode_topic_declaration(std::span<const std::byte>{bytes}.first(n)).has_value());
}

TEST_CASE("topic_declaration codec: an over-cap type_name decodes to nullopt")
{
    const topic_declaration td{.topic_hash = 1, .type_id = 2, .type_name = std::string(k_max_type_name + 1, 'T'), .state = type_state::declared};

    REQUIRE(!decode_topic_declaration(encode_topic_declaration(td)).has_value());
}

TEST_CASE("topic_declaration codec: an unknown type_state byte decodes to nullopt")
{
    const topic_declaration td{.topic_hash = 1, .type_id = 2, .type_name = "sensor/Imu", .state = type_state::declared};
    auto bytes = encode_topic_declaration(td);
    // The state byte sits behind topic_hash + type_id.
    bytes[sizeof(std::uint64_t) * 2] = std::byte{0x7F};

    REQUIRE(!decode_topic_declaration(bytes).has_value());
}

TEST_CASE("topic_declaration codec: the msg_type slot is additive (0x0D, no protocol bump)")
{
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::declare) == 0x0D);
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::heartbeat) == 0x0C);
}
