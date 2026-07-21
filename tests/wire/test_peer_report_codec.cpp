// The peer-report codec oracle: an untrusted-input decoder for a relay's re-announcement of a
// third-party origin. It proves the byte-exact round-trip (fixed prefix + the two presence-flagged
// trailing blocks), pins the wire layout offset-by-offset, and proves the hardened-decode set — a
// truncated frame, an over-cap universe pattern, an over-cap per-topic string, an oversized topics
// count, or an unknown type_state byte each yield nullopt with no partial struct.

#include "plexus/wire/frame.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/udp_dedup_window.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <type_traits>

using plexus::node_id;
using plexus::wire::type_state;
using plexus::wire::peer_report;
using plexus::wire::topic_declaration;
using plexus::wire::encode_peer_report;
using plexus::wire::decode_peer_report;
using plexus::wire::udp_dedup_window;
using plexus::wire::k_peer_report_consent_flag;
using plexus::wire::k_peer_report_withdrawal_flag;
using plexus::wire::k_peer_report_topics_flag;
using plexus::wire::k_peer_report_universe_pattern_flag;
using plexus::wire::k_peer_report_universe_pattern_max;
using plexus::wire::detail::k_max_fqn;
using plexus::wire::detail::k_max_type_name;

namespace {

node_id make_origin()
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(0xB0 + i);
    return id;
}

peer_report make_sample()
{
    peer_report pr;
    pr.origin          = make_origin();
    pr.origin_universe = 0x11223344u;
    pr.hop             = 7;
    pr.seq             = 0xABCD;
    pr.flags           = 0;
    return pr;
}

topic_declaration make_topic(type_state state, std::string name)
{
    return topic_declaration{.topic_hash = 0x0102030405060708ULL, .type_id = 0x1112131415161718ULL, .fqn = "sensor/imu", .type_name = std::move(name), .state = state};
}

}

static_assert(std::is_same_v<decltype(peer_report::seq), udp_dedup_window::seq_t>,
              "peer_report::seq must match the udp_dedup_window sequence type so a per-origin "
              "dedup window admits the report's seq directly");

TEST_CASE("peer_report codec: the fixed-prefix report round-trips byte-exactly", "[wire][peer_report]")
{
    const auto pr    = make_sample();
    const auto bytes = encode_peer_report(pr);

    // Wire layout pin: origin(16) + universe(4) + hop(1) + seq(2) + flags(1) = 24, no trailing.
    REQUIRE(bytes.size() == 24);
    for(std::size_t i = 0; i < 16; ++i)
        REQUIRE(bytes[i] == static_cast<std::byte>(0xB0 + i));
    REQUIRE(bytes[16] == std::byte{0x11});
    REQUIRE(bytes[17] == std::byte{0x22});
    REQUIRE(bytes[18] == std::byte{0x33});
    REQUIRE(bytes[19] == std::byte{0x44});
    REQUIRE(bytes[20] == std::byte{0x07});
    REQUIRE(bytes[21] == std::byte{0xAB});
    REQUIRE(bytes[22] == std::byte{0xCD});
    REQUIRE(bytes[23] == std::byte{0x00});

    const auto decoded = decode_peer_report(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == pr);
}

TEST_CASE("peer_report codec: the consent and withdrawal bits round-trip independently", "[wire][peer_report]")
{
    auto pr    = make_sample();
    pr.flags   = k_peer_report_consent_flag | k_peer_report_withdrawal_flag;

    const auto decoded = decode_peer_report(encode_peer_report(pr));
    REQUIRE(decoded.has_value());
    REQUIRE((decoded->flags & k_peer_report_consent_flag) != 0);
    REQUIRE((decoded->flags & k_peer_report_withdrawal_flag) != 0);
    REQUIRE(*decoded == pr);
}

TEST_CASE("peer_report codec: an origin universe pattern round-trips when its flag is set", "[wire][peer_report]")
{
    auto pr                     = make_sample();
    pr.origin_universe_pattern  = "factory/line/*";
    pr.flags                   |= k_peer_report_universe_pattern_flag;

    const auto decoded = decode_peer_report(encode_peer_report(pr));
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->origin_universe_pattern == "factory/line/*");
    REQUIRE(*decoded == pr);
}

TEST_CASE("peer_report codec: a topics-with-types list round-trips every type state", "[wire][peer_report]")
{
    auto pr   = make_sample();
    pr.flags |= k_peer_report_topics_flag;
    pr.topics = {make_topic(type_state::undeclared, ""), make_topic(type_state::declared_empty, ""), make_topic(type_state::declared, "geometry/Pose")};

    const auto decoded = decode_peer_report(encode_peer_report(pr));
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->topics.size() == 3);
    REQUIRE(*decoded == pr);
}

TEST_CASE("peer_report codec: consent, pattern, and topics all round-trip together", "[wire][peer_report]")
{
    auto pr                     = make_sample();
    pr.origin_universe_pattern  = "site/*/gateway";
    pr.flags                    = k_peer_report_consent_flag | k_peer_report_universe_pattern_flag | k_peer_report_topics_flag;
    pr.topics                   = {make_topic(type_state::declared, std::string(300, 'T'))};

    const auto decoded = decode_peer_report(encode_peer_report(pr));
    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == pr);
}

TEST_CASE("peer_report codec: every truncation of a full frame decodes to nullopt", "[wire][peer_report]")
{
    auto pr                     = make_sample();
    pr.origin_universe_pattern  = "site/*/gateway";
    pr.flags                    = k_peer_report_universe_pattern_flag | k_peer_report_topics_flag;
    pr.topics                   = {make_topic(type_state::declared, "geometry/Pose")};
    const auto bytes            = encode_peer_report(pr);

    for(std::size_t n = 0; n < bytes.size(); ++n)
        REQUIRE(!decode_peer_report(std::span<const std::byte>{bytes}.first(n)).has_value());
}

TEST_CASE("peer_report codec: an empty span decodes to nullopt", "[wire][peer_report]")
{
    REQUIRE(!decode_peer_report(std::span<const std::byte>{}).has_value());
}

TEST_CASE("peer_report codec: an over-cap universe pattern decodes to nullopt before any copy", "[wire][peer_report]")
{
    auto pr                     = make_sample();
    pr.origin_universe_pattern  = std::string(k_peer_report_universe_pattern_max + 1, 'x');
    pr.flags                   |= k_peer_report_universe_pattern_flag;

    REQUIRE(!decode_peer_report(encode_peer_report(pr)).has_value());
}

TEST_CASE("peer_report codec: an over-cap topic fqn decodes to nullopt", "[wire][peer_report]")
{
    auto pr   = make_sample();
    pr.flags |= k_peer_report_topics_flag;
    pr.topics = {topic_declaration{.topic_hash = 1, .type_id = 2, .fqn = std::string(k_max_fqn + 1, 'f'), .type_name = "sensor/Imu", .state = type_state::declared}};

    REQUIRE(!decode_peer_report(encode_peer_report(pr)).has_value());
}

TEST_CASE("peer_report codec: an over-cap topic type_name decodes to nullopt", "[wire][peer_report]")
{
    auto pr   = make_sample();
    pr.flags |= k_peer_report_topics_flag;
    pr.topics = {make_topic(type_state::declared, std::string(k_max_type_name + 1, 'T'))};

    REQUIRE(!decode_peer_report(encode_peer_report(pr)).has_value());
}

TEST_CASE("peer_report codec: an unknown topic type_state byte decodes to nullopt", "[wire][peer_report]")
{
    auto pr   = make_sample();
    pr.flags |= k_peer_report_topics_flag;
    pr.topics = {make_topic(type_state::declared, "sensor/Imu")};
    auto bytes = encode_peer_report(pr);

    // Fixed prefix(24) + n_topics(2) + topic_hash(8) + type_id(8), then the state byte.
    bytes[24 + 2 + 8 + 8] = std::byte{0x7F};
    REQUIRE(!decode_peer_report(bytes).has_value());
}

TEST_CASE("peer_report codec: the msg_type slot is additive (0x0E after declare 0x0D, no protocol bump)", "[wire][peer_report]")
{
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::peer_report) == 0x0E);
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::declare) == 0x0D);
}
