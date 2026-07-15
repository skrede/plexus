#include "test_wire_codec_common.h"

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
            std::byte{0x01},                                                                                                                        // source = publisher
            std::byte{0x00}, std::byte{0x02}, std::byte{0x2F}, std::byte{0x6E},                                                                     // fqn_len(2) + "/n"
            std::byte{0x00}, std::byte{0x00}};                                                                                                      // type_name_len(2) + ""

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
