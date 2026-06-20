#include "test_wire_codec_common.h"

// --- Data payload codec tests ---

TEST_CASE("unidirectional payload size is 17 + data", "[wire][data]")
{
    unidirectional_header hdr{
            .source = endpoint_source_type::publisher, .sequence = 1, .topic_hash = 0xDEAD};
    std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};

    auto encoded = encode_unidirectional(hdr, data);
    CHECK(encoded.size() == 17 + 3);
}

TEST_CASE("unidirectional round-trip preserves fields and data", "[wire][data]")
{
    unidirectional_header hdr{
            .source = endpoint_source_type::publisher, .sequence = 42, .topic_hash = 0xDEAD};
    std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};

    auto encoded = encode_unidirectional(hdr, data);
    auto decoded = decode_unidirectional(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->header.source == endpoint_source_type::publisher);
    CHECK(decoded->header.sequence == 42);
    CHECK(decoded->header.topic_hash == 0xDEAD);
    REQUIRE(decoded->data.size() == 3);
    CHECK(decoded->data[0] == std::byte{0x01});
    CHECK(decoded->data[1] == std::byte{0x02});
    CHECK(decoded->data[2] == std::byte{0x03});
}

TEST_CASE("unidirectional round-trips a flag-gated source-identity counter", "[wire][data][gid]")
{
    unidirectional_header hdr{
            .source = endpoint_source_type::publisher, .sequence = 7, .topic_hash = 0xCAFE};
    std::vector<std::byte>  data{std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};
    constexpr std::uint64_t counter = 0x1234; // a multi-byte varint

    auto encoded = encode_unidirectional(hdr, data, counter);
    auto decoded = decode_unidirectional(encoded, /*has_source_identity=*/true);

    REQUIRE(decoded.has_value());
    REQUIRE(decoded->endpoint_counter.has_value());
    CHECK(*decoded->endpoint_counter == counter);
    CHECK(decoded->header.topic_hash == 0xCAFE);
    REQUIRE(decoded->data.size() == 3);
    CHECK(decoded->data[0] == std::byte{0xAA});
    CHECK(decoded->data[2] == std::byte{0xCC});
}

TEST_CASE("unidirectional flag-clear encode is byte-identical to a no-counter frame",
          "[wire][data][gid]")
{
    unidirectional_header hdr{
            .source = endpoint_source_type::publisher, .sequence = 42, .topic_hash = 0xDEAD};
    std::vector<std::byte> data{std::byte{0x01}, std::byte{0x02}};

    auto without = encode_unidirectional(hdr, data);               // no counter argument
    auto clear   = encode_unidirectional(hdr, data, std::nullopt); // explicitly absent
    CHECK(without == clear);

    // Flag-clear decode yields no counter and the data immediately after the 17B header.
    auto decoded = decode_unidirectional(without);
    REQUIRE(decoded.has_value());
    CHECK_FALSE(decoded->endpoint_counter.has_value());
    REQUIRE(decoded->data.size() == 2);
}

TEST_CASE("unidirectional decode rejects a truncated source-identity region", "[wire][data][gid]")
{
    unidirectional_header hdr{
            .source = endpoint_source_type::publisher, .sequence = 1, .topic_hash = 0xBEEF};
    // A 17B header followed by a lone continuation byte (0x80) with no terminator:
    // read_varint must return nullopt, so the whole decode fails (warn-and-drop) —
    // never an over-read past the buffer.
    std::vector<std::byte> truncated = encode_unidirectional(hdr, {});
    truncated.push_back(std::byte{0x80});

    auto decoded = decode_unidirectional(truncated, /*has_source_identity=*/true);
    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("bidirectional round-trip preserves all fields", "[wire][data]")
{
    bidirectional_header   hdr{.source         = endpoint_source_type::caller,
                               .sequence       = 99,
                               .topic_hash     = 0xAAAABBBBCCCCDDDDULL,
                               .type_hash_1    = 0x1111222233334444ULL,
                               .type_hash_2    = 0x5555666677778888ULL,
                               .correlation_id = 12345};
    std::vector<std::byte> data{std::byte{0xFE}, std::byte{0xED}};

    auto encoded = encode_bidirectional(hdr, data);
    CHECK(encoded.size() == 41 + 2);

    auto decoded = decode_bidirectional(encoded);
    REQUIRE(decoded.has_value());
    CHECK(decoded->header.source == endpoint_source_type::caller);
    CHECK(decoded->header.sequence == 99);
    CHECK(decoded->header.topic_hash == 0xAAAABBBBCCCCDDDDULL);
    CHECK(decoded->header.type_hash_1 == 0x1111222233334444ULL);
    CHECK(decoded->header.type_hash_2 == 0x5555666677778888ULL);
    CHECK(decoded->header.correlation_id == 12345);
    REQUIRE(decoded->data.size() == 2);
    CHECK(decoded->data[0] == std::byte{0xFE});
    CHECK(decoded->data[1] == std::byte{0xED});
}

TEST_CASE("unidirectional decode fails on short payload", "[wire][data]")
{
    std::array<std::byte, 16> short_buf{};
    auto                      result = decode_unidirectional(short_buf);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("bidirectional decode fails on short payload", "[wire][data]")
{
    std::array<std::byte, 40> short_buf{};
    auto                      result = decode_bidirectional(short_buf);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("full frame round-trip with unidirectional payload", "[wire][data]")
{
    unidirectional_header uni_hdr{
            .source = endpoint_source_type::signal, .sequence = 7, .topic_hash = 0xCAFE};
    std::vector<std::byte> data{std::byte{0xAA}, std::byte{0xBB}};

    auto payload = encode_unidirectional(uni_hdr, data);

    frame_header fhdr{.type         = msg_type::unidirectional,
                      .flags        = 0,
                      .session_id   = 0,
                      .timestamp_ns = 1000000,
                      .payload_len  = 0};
    auto         frame = encode_frame(fhdr, payload);

    CHECK(frame.size() == 28 + payload.size());

    auto decoded_hdr = decode_header(std::span<const std::byte>(frame));
    REQUIRE(decoded_hdr.has_value());
    CHECK(decoded_hdr->type == msg_type::unidirectional);
    CHECK(decoded_hdr->payload_len == payload.size());

    auto payload_span = std::span<const std::byte>(frame).subspan(header_size);
    auto decoded_uni  = decode_unidirectional(payload_span);
    REQUIRE(decoded_uni.has_value());
    CHECK(decoded_uni->header.source == endpoint_source_type::signal);
    CHECK(decoded_uni->header.sequence == 7);
    CHECK(decoded_uni->header.topic_hash == 0xCAFE);
    REQUIRE(decoded_uni->data.size() == 2);
    CHECK(decoded_uni->data[0] == std::byte{0xAA});
    CHECK(decoded_uni->data[1] == std::byte{0xBB});
}
