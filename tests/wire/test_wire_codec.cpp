#include "test_wire_codec_common.h"

// --- Frame header codec tests ---

TEST_CASE("encode_header produces 28 bytes", "[wire][codec]")
{
    frame_header hdr{.type         = msg_type::unidirectional,
                     .flags        = 0,
                     .session_id   = 0,
                     .timestamp_ns = 0,
                     .payload_len  = 0};
    auto         buf = encode_header(hdr);
    CHECK(buf.size() == 28);
}

TEST_CASE("encoded header starts with magic 0x56 0x50", "[wire][codec]")
{
    frame_header hdr{.type         = msg_type::unidirectional,
                     .flags        = 0,
                     .session_id   = 0,
                     .timestamp_ns = 0,
                     .payload_len  = 0};
    auto         buf = encode_header(hdr);
    CHECK(buf[0] == std::byte{0x56});
    CHECK(buf[1] == std::byte{0x50});
}

TEST_CASE("header round-trip preserves all fields", "[wire][codec]")
{
    frame_header hdr{.type  = msg_type::bidirectional,
                     .flags = 0x3F,
                     // A value with bits set above byte 0 — proves the u64 session_id survives
                     // the round-trip with no truncation (the u8-narrowing tell would drop the
                     // high bytes).
                     .session_id   = 0xA1B2C3D4E5F60789ULL,
                     .timestamp_ns = 0xAABBCCDDEEFF0011ULL,
                     .payload_len  = 0x1234567890ABCDEFULL};

    auto buf     = encode_header(hdr);
    auto decoded = decode_header(buf);

    REQUIRE(decoded.has_value());
    CHECK(decoded->type == msg_type::bidirectional);
    CHECK(decoded->flags == 0x3F);
    CHECK(decoded->session_id == 0xA1B2C3D4E5F60789ULL);
    CHECK(decoded->timestamp_ns == 0xAABBCCDDEEFF0011ULL);
    CHECK(decoded->payload_len == 0x1234567890ABCDEFULL);
}

TEST_CASE("decode_header returns nullopt for short data", "[wire][codec]")
{
    std::array<std::byte, 27> short_buf{}; // one below the 28-byte header_size
    auto                      result = decode_header(short_buf);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("decode_header returns nullopt for bad magic", "[wire][codec]")
{
    frame_header hdr{.type         = msg_type::unidirectional,
                     .flags        = 0,
                     .session_id   = 0,
                     .timestamp_ns = 0,
                     .payload_len  = 0};
    auto         buf = encode_header(hdr);
    buf[0]           = std::byte{0xFF};
    auto result      = decode_header(buf);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("header fields are big-endian on wire", "[wire][codec]")
{
    frame_header hdr{.type         = msg_type::unidirectional,
                     .flags        = 0,
                     .session_id   = 0x1122334455667788ULL,
                     .timestamp_ns = 0x0102030405060708ULL,
                     .payload_len  = 0};

    auto buf = encode_header(hdr);

    // session_id is the big-endian u64 at offset 4; timestamp_ns the u64 at offset 12.
    CHECK(buf[4] == std::byte{0x11});
    CHECK(buf[5] == std::byte{0x22});
    CHECK(buf[6] == std::byte{0x33});
    CHECK(buf[7] == std::byte{0x44});
    CHECK(buf[8] == std::byte{0x55});
    CHECK(buf[9] == std::byte{0x66});
    CHECK(buf[10] == std::byte{0x77});
    CHECK(buf[11] == std::byte{0x88});
    CHECK(buf[12] == std::byte{0x01});
    CHECK(buf[13] == std::byte{0x02});
    CHECK(buf[14] == std::byte{0x03});
    CHECK(buf[15] == std::byte{0x04});
    CHECK(buf[16] == std::byte{0x05});
    CHECK(buf[17] == std::byte{0x06});
    CHECK(buf[18] == std::byte{0x07});
    CHECK(buf[19] == std::byte{0x08});
}

TEST_CASE("encode_frame concatenates header and payload", "[wire][codec]")
{
    frame_header           hdr{.type         = msg_type::subscribe,
                               .flags        = 0,
                               .session_id   = 0,
                               .timestamp_ns = 100,
                               .payload_len  = 0};
    std::vector<std::byte> payload{std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};

    auto frame = encode_frame(hdr, payload);
    CHECK(frame.size() == 28 + 3);

    auto decoded = decode_header(frame);
    REQUIRE(decoded.has_value());
    CHECK(decoded->payload_len == 3);
}

TEST_CASE("msg_type enum values", "[wire][codec]")
{
    CHECK(static_cast<uint8_t>(msg_type::unidirectional) == 0x01);
    CHECK(static_cast<uint8_t>(msg_type::bidirectional) == 0x02);
    CHECK(static_cast<uint8_t>(msg_type::handshake_req) == 0x03);
    CHECK(static_cast<uint8_t>(msg_type::handshake_resp) == 0x04);
    CHECK(static_cast<uint8_t>(msg_type::subscribe) == 0x05);
    CHECK(static_cast<uint8_t>(msg_type::unsubscribe) == 0x06);
    CHECK(static_cast<uint8_t>(msg_type::fetch_latched) == 0x07);
    CHECK(static_cast<uint8_t>(msg_type::fetch_metadata) == 0x08);
}

TEST_CASE("endpoint_source_type enum values", "[wire][codec]")
{
    CHECK(static_cast<uint8_t>(endpoint_source_type::publisher) == 0x01);
    CHECK(static_cast<uint8_t>(endpoint_source_type::signal) == 0x03);
    CHECK(static_cast<uint8_t>(endpoint_source_type::attribute) == 0x05);
    CHECK(static_cast<uint8_t>(endpoint_source_type::caller) == 0x06);
    CHECK(static_cast<uint8_t>(endpoint_source_type::procedure) == 0x07);
    CHECK(static_cast<uint8_t>(endpoint_source_type::plexus) == 0x08);
}
