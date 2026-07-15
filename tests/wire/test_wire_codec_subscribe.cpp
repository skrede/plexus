#include "test_wire_codec_common.h"

// --- Subscribe/unsubscribe codec tests ---

TEST_CASE("subscribe request round-trip", "[wire][subscribe]")
{
    subscribe_request req{.fqn = "/node/topic", .type_name = "MyMessage", .topic_hash = 0xDEADBEEF, .type_hash = 0xCAFEBABE, .source = endpoint_source_type::publisher};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->fqn == "/node/topic");
    CHECK(decoded->type_name == "MyMessage");
    CHECK(decoded->topic_hash == 0xDEADBEEF);
    CHECK(decoded->type_hash == 0xCAFEBABE);
    CHECK(decoded->source == endpoint_source_type::publisher);
}

TEST_CASE("subscribe response round-trip", "[wire][subscribe]")
{
    subscribe_response resp{.topic_hash = 0xDEADBEEF, .status = subscribe_status::subscribed};

    auto encoded = encode_subscribe_response(resp);
    auto decoded = decode_subscribe_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == 0xDEADBEEF);
    CHECK(decoded->status == subscribe_status::subscribed);
}

TEST_CASE("subscribe request with long strings", "[wire][subscribe]")
{
    subscribe_request req{.fqn = std::string(500, '/'), .type_name = std::string(300, 'T'), .topic_hash = 0xABCD, .type_hash = 0xEF01, .source = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->fqn.size() == 500);
    CHECK(decoded->type_name.size() == 300);
}

TEST_CASE("subscribe decode fails on truncated payload", "[wire][subscribe]")
{
    std::array<std::byte, 10> short_buf{};
    auto result = decode_subscribe_request(short_buf);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("unsubscribe request round-trip", "[wire][subscribe]")
{
    unsubscribe_request req{.topic_hash = 0x1234};

    auto encoded = encode_unsubscribe_request(req);
    auto decoded = decode_unsubscribe_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == 0x1234);
}

TEST_CASE("unsubscribe response round-trip", "[wire][subscribe]")
{
    unsubscribe_response resp{.topic_hash = 0x1234, .status = unsubscribe_status::destroyed};

    auto encoded = encode_unsubscribe_response(resp);
    auto decoded = decode_unsubscribe_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == 0x1234);
    CHECK(decoded->status == unsubscribe_status::destroyed);
}

TEST_CASE("subscribe_status enum values", "[wire][subscribe]")
{
    CHECK(static_cast<uint8_t>(subscribe_status::subscribed) == 0x01);
    CHECK(static_cast<uint8_t>(subscribe_status::created) == 0x02);
    CHECK(static_cast<uint8_t>(subscribe_status::type_mismatch) == 0x03);
    CHECK(static_cast<uint8_t>(subscribe_status::already_subscribed) == 0x04);
}

TEST_CASE("unsubscribe_status enum values", "[wire][subscribe]")
{
    CHECK(static_cast<uint8_t>(unsubscribe_status::unsubscribed) == 0x01);
    CHECK(static_cast<uint8_t>(unsubscribe_status::destroyed) == 0x02);
    CHECK(static_cast<uint8_t>(unsubscribe_status::not_subscribed) == 0x03);
}

TEST_CASE("subscribe response with type_mismatch", "[wire][subscribe]")
{
    subscribe_response resp{.topic_hash = 0xBEEF, .status = subscribe_status::type_mismatch};

    auto encoded = encode_subscribe_response(resp);
    auto decoded = decode_subscribe_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->status == subscribe_status::type_mismatch);
}

TEST_CASE("subscribe response with created status", "[wire][subscribe]")
{
    subscribe_response resp{.topic_hash = 0xCAFE, .status = subscribe_status::created};

    auto encoded = encode_subscribe_response(resp);
    auto decoded = decode_subscribe_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->status == subscribe_status::created);
}

// --- subscribe oversized-fqn / oversized-type_name rejection ---
//
// These tests pin the inline policy caps that decode_subscribe_request applies
// to the uint16_t-prefixed fqn and type_name fields. The encoder happily emits
// frames whose lengths exceed those caps (the encoder is local-trust only),
// which is precisely the surface the decoder rejects on the way back in: any
// peer can synthesize a frame asking us to allocate a 65 KB string per field,
// and the inline cap forces a nullopt return well below that.
//
// The literal 1024 / 512 values mirror the file-private k_max_fqn /
// k_max_type_name in subscribe.h's detail namespace; the test does not import
// the constants, it asserts the contract at the documented bound.

TEST_CASE("decode_subscribe_request rejects oversized fqn beyond k_max_fqn", "[wire][subscribe][oob]")
{
    subscribe_request req{.fqn = std::string(1025, '/'), .type_name = "T", .topic_hash = 0xABCD, .type_hash = 0xEF01, .source = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("decode_subscribe_request rejects oversized type_name beyond k_max_type_name", "[wire][subscribe][oob]")
{
    subscribe_request req{.fqn = "/topic", .type_name = std::string(513, 'T'), .topic_hash = 0xABCD, .type_hash = 0xEF01, .source = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("decode_subscribe_request accepts fqn and type_name at the cap boundary", "[wire][subscribe][oob]")
{
    subscribe_request req{.fqn = std::string(1024, '/'), .type_name = std::string(512, 'T'), .topic_hash = 0xABCD, .type_hash = 0xEF01, .source = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->fqn.size() == 1024);
    CHECK(decoded->type_name.size() == 512);
}

// --- type-declaration three-state (undeclared / declared-empty / declared) ---
//
// A bare subscribe frame that declares no type must stay byte-identical to the
// pre-flag layout: the flag byte only appears when a type is asserted, and it
// trails the whole frame (after the optional QoS region), so the fixed head and
// both string fields never shift.

TEST_CASE("undeclared subscribe frame is byte-identical to the pre-flag layout", "[wire][subscribe]")
{
    subscribe_request req{.fqn = "/n", .type_name = "", .topic_hash = 0x0102030405060708ULL, .type_hash = 0x1112131415161718ULL, .source = endpoint_source_type::publisher};

    const std::vector<std::byte> golden{
            std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}, std::byte{0x05}, std::byte{0x06}, std::byte{0x07}, std::byte{0x08}, // topic_hash
            std::byte{0x11}, std::byte{0x12}, std::byte{0x13}, std::byte{0x14}, std::byte{0x15}, std::byte{0x16}, std::byte{0x17}, std::byte{0x18}, // type_hash
            std::byte{0x01},                                                                                                                       // source = publisher
            std::byte{0x00}, std::byte{0x02}, std::byte{0x2F}, std::byte{0x6E},                                                                     // fqn_len(2) + "/n"
            std::byte{0x00}, std::byte{0x00}};                                                                                                     // type_name_len(2) + ""

    auto encoded = encode_subscribe_request(req);

    CHECK(encoded == golden);

    auto decoded = decode_subscribe_request(encoded);
    REQUIRE(decoded.has_value());
    CHECK_FALSE(decoded->type_declared);
}

TEST_CASE("subscribe frame round-trips all three type-declaration states", "[wire][subscribe]")
{
    subscribe_request base{.fqn = "/n/t", .type_name = "", .topic_hash = 0xABCD, .type_hash = 0xEF01, .source = endpoint_source_type::publisher};

    SECTION("undeclared")
    {
        base.type_declared = false;
        base.type_name     = "";
        auto decoded       = decode_subscribe_request(encode_subscribe_request(base));
        REQUIRE(decoded.has_value());
        CHECK_FALSE(decoded->type_declared);
    }
    SECTION("declared-empty")
    {
        base.type_declared = true;
        base.type_name     = "";
        auto decoded       = decode_subscribe_request(encode_subscribe_request(base));
        REQUIRE(decoded.has_value());
        CHECK(decoded->type_declared);
        CHECK(decoded->type_name.empty());
    }
    SECTION("declared with a name")
    {
        base.type_declared = true;
        base.type_name     = "geometry/Pose";
        auto decoded       = decode_subscribe_request(encode_subscribe_request(base));
        REQUIRE(decoded.has_value());
        CHECK(decoded->type_declared);
        CHECK(decoded->type_name == "geometry/Pose");
    }
}

TEST_CASE("subscribe declared-type flag reads correctly when it trails the QoS region", "[wire][subscribe]")
{
    subscribe_request req{.fqn = "/n/t", .type_name = "sensor/Imu", .topic_hash = 0xABCD, .type_hash = 0xEF01, .source = endpoint_source_type::publisher};
    req.type_declared     = true;
    req.has_qos           = true;
    req.qos.delivery_mode = 1;
    req.qos.replay_depth  = 9;

    auto decoded = decode_subscribe_request(encode_subscribe_request(req));

    REQUIRE(decoded.has_value());
    REQUIRE(decoded->has_qos);
    CHECK(decoded->qos.delivery_mode == 1);
    CHECK(decoded->qos.replay_depth == 9);
    CHECK(decoded->type_declared);
    CHECK(decoded->type_name == "sensor/Imu");
}
