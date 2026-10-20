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

TEST_CASE("subscribe response carries the offer state beside the degraded byte", "[wire][subscribe]")
{
    for(const offer_state offer : {offer_state::no_publisher, offer_state::offered, offer_state::withheld})
    {
        const auto plain = encode_subscribe_response({.topic_hash = 0xFEED, .status = subscribe_status::subscribed, .offer = offer});
        const auto degraded =
                encode_subscribe_response({.topic_hash = 0xFEED, .status = subscribe_status::subscribed_degraded, .offer = offer, .has_degraded = true, .degraded_flags = 0x05});
        const auto plain_decoded    = decode_subscribe_response(plain);
        const auto degraded_decoded = decode_subscribe_response(degraded);

        REQUIRE(plain.size() == subscribe_response_size);
        REQUIRE(degraded.size() == subscribe_response_size + 1);
        REQUIRE(plain_decoded.has_value());
        REQUIRE(degraded_decoded.has_value());
        CHECK(plain_decoded->offer == offer);
        CHECK_FALSE(plain_decoded->has_degraded);
        CHECK(degraded_decoded->offer == offer);
        CHECK(degraded_decoded->has_degraded);
        CHECK(degraded_decoded->degraded_flags == 0x05);
    }
}

TEST_CASE("subscribe response with an offer state past withheld is refused", "[wire][subscribe]")
{
    auto encoded = encode_subscribe_response({.topic_hash = 0xFEED, .status = subscribe_status::subscribed, .offer = offer_state::withheld});
    REQUIRE(decode_subscribe_response(encoded).has_value());
    for(const std::uint8_t value : {std::uint8_t{0x03}, std::uint8_t{0xFF}})
    {
        encoded[9] = std::byte{value};
        CHECK_FALSE(decode_subscribe_response(encoded).has_value());
    }
}

TEST_CASE("subscribe response shorter than the offer byte is refused", "[wire][subscribe]")
{
    const auto encoded = encode_subscribe_response({.topic_hash = 0xFEED, .status = subscribe_status::subscribed, .offer = offer_state::offered});
    CHECK_FALSE(decode_subscribe_response(std::span<const std::byte>{encoded.data(), subscribe_response_size - 1}).has_value());
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
