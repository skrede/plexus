#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace plexus::wire;

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

// --- Subscribe/unsubscribe codec tests ---

TEST_CASE("subscribe request round-trip", "[wire][subscribe]")
{
    subscribe_request req{.fqn        = "/node/topic",
                          .type_name  = "MyMessage",
                          .topic_hash = 0xDEADBEEF,
                          .type_hash  = 0xCAFEBABE,
                          .source     = endpoint_source_type::publisher};

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
    subscribe_request req{.fqn        = std::string(500, '/'),
                          .type_name  = std::string(300, 'T'),
                          .topic_hash = 0xABCD,
                          .type_hash  = 0xEF01,
                          .source     = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->fqn.size() == 500);
    CHECK(decoded->type_name.size() == 300);
}

TEST_CASE("subscribe decode fails on truncated payload", "[wire][subscribe]")
{
    std::array<std::byte, 10> short_buf{};
    auto                      result = decode_subscribe_request(short_buf);
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

TEST_CASE("decode_subscribe_request rejects oversized fqn beyond k_max_fqn",
          "[wire][subscribe][oob]")
{
    subscribe_request req{.fqn        = std::string(1025, '/'),
                          .type_name  = "T",
                          .topic_hash = 0xABCD,
                          .type_hash  = 0xEF01,
                          .source     = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("decode_subscribe_request rejects oversized type_name beyond k_max_type_name",
          "[wire][subscribe][oob]")
{
    subscribe_request req{.fqn        = "/topic",
                          .type_name  = std::string(513, 'T'),
                          .topic_hash = 0xABCD,
                          .type_hash  = 0xEF01,
                          .source     = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("decode_subscribe_request accepts fqn and type_name at the cap boundary",
          "[wire][subscribe][oob]")
{
    subscribe_request req{.fqn        = std::string(1024, '/'),
                          .type_name  = std::string(512, 'T'),
                          .topic_hash = 0xABCD,
                          .type_hash  = 0xEF01,
                          .source     = endpoint_source_type::signal};

    auto encoded = encode_subscribe_request(req);
    auto decoded = decode_subscribe_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->fqn.size() == 1024);
    CHECK(decoded->type_name.size() == 512);
}

// --- RPC request/response codec tests ---

TEST_CASE("RPC request frame: round-trip encode/decode", "[wire][rpc]")
{
    bidirectional_header     hdr{.source         = endpoint_source_type::caller,
                                 .sequence       = 1,
                                 .topic_hash     = 0xAAAA,
                                 .type_hash_1    = 0xBBBB,
                                 .type_hash_2    = 0xCCCC,
                                 .correlation_id = 42};
    std::array<std::byte, 4> param_data{std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
                                        std::byte{0x04}};

    auto encoded = encode_rpc_request(hdr, param_data);
    auto decoded = decode_rpc_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->header.source == endpoint_source_type::caller);
    CHECK(decoded->header.topic_hash == 0xAAAA);
    CHECK(decoded->header.type_hash_1 == 0xBBBB);
    CHECK(decoded->header.type_hash_2 == 0xCCCC);
    CHECK(decoded->header.correlation_id == 42);
    REQUIRE(decoded->param_data.size() == 4);
    CHECK(decoded->param_data[0] == std::byte{0x01});
}

TEST_CASE("RPC response frame: round-trip with success status", "[wire][rpc]")
{
    bidirectional_header     hdr{.source         = endpoint_source_type::procedure,
                                 .sequence       = 2,
                                 .topic_hash     = 0xAAAA,
                                 .type_hash_1    = 0xCCCC,
                                 .type_hash_2    = 0xBBBB,
                                 .correlation_id = 42};
    std::array<std::byte, 4> return_data{std::byte{0x10}, std::byte{0x20}, std::byte{0x30},
                                         std::byte{0x40}};

    auto encoded = encode_rpc_response(hdr, rpc_status::success, return_data);
    // exactly one status byte prepended to the body
    CHECK(encoded.size() == bidirectional_header_size + 1 + return_data.size());

    auto decoded = decode_rpc_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->header.source == endpoint_source_type::procedure);
    CHECK(decoded->header.correlation_id == 42);
    CHECK(decoded->status == rpc_status::success);
    REQUIRE(decoded->return_data.size() == 4);
    CHECK(decoded->return_data[0] == std::byte{0x10});
}

TEST_CASE("RPC response frame: round-trip with error status", "[wire][rpc]")
{
    bidirectional_header hdr{.source         = endpoint_source_type::procedure,
                             .sequence       = 3,
                             .topic_hash     = 0xAAAA,
                             .type_hash_1    = 0xCCCC,
                             .type_hash_2    = 0xBBBB,
                             .correlation_id = 42};

    auto encoded = encode_rpc_response(hdr, rpc_status::no_handler, {});
    auto decoded = decode_rpc_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->status == rpc_status::no_handler);
    CHECK(decoded->return_data.empty());
}

TEST_CASE("RPC response frame: zero-alloc _into matches the allocating encode", "[wire][rpc]")
{
    bidirectional_header     hdr{.source         = endpoint_source_type::procedure,
                                 .sequence       = 11,
                                 .topic_hash     = 0x1234,
                                 .type_hash_1    = 0xCCCC,
                                 .type_hash_2    = 0xBBBB,
                                 .correlation_id = 7};
    std::array<std::byte, 3> return_data{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}};

    auto                   allocating = encode_rpc_response(hdr, rpc_status::success, return_data);
    std::vector<std::byte> reused;
    encode_rpc_response_into(reused, hdr, rpc_status::success, return_data);
    CHECK(reused == allocating);

    auto                   req_allocating = encode_rpc_request(hdr, return_data);
    std::vector<std::byte> req_reused;
    encode_rpc_request_into(req_reused, hdr, return_data);
    CHECK(req_reused == req_allocating);
}

TEST_CASE("RPC response frame: status byte above the highest enumerator returns nullopt",
          "[wire][rpc]")
{
    bidirectional_header     hdr{.source         = endpoint_source_type::procedure,
                                 .sequence       = 4,
                                 .topic_hash     = 0xAAAA,
                                 .type_hash_1    = 0xCCCC,
                                 .type_hash_2    = 0xBBBB,
                                 .correlation_id = 42};
    std::array<std::byte, 4> return_data{std::byte{0x10}, std::byte{0x20}, std::byte{0x30},
                                         std::byte{0x40}};

    auto encoded                       = encode_rpc_response(hdr, rpc_status::success, return_data);
    encoded[bidirectional_header_size] = std::byte{0xFF};
    auto decoded                       = decode_rpc_response(encoded);

    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("RPC response frame: in-range reserved-gap status byte returns nullopt", "[wire][rpc]")
{
    bidirectional_header     hdr{.source         = endpoint_source_type::procedure,
                                 .sequence       = 6,
                                 .topic_hash     = 0xAAAA,
                                 .type_hash_1    = 0xCCCC,
                                 .type_hash_2    = 0xBBBB,
                                 .correlation_id = 42};
    std::array<std::byte, 2> return_data{std::byte{0x10}, std::byte{0x20}};

    // 6 sits below the highest enumerator (20) but is a reserved gap with no
    // defined rpc_status — the decoder must reject it, never yield an undefined value.
    auto encoded                       = encode_rpc_response(hdr, rpc_status::success, return_data);
    encoded[bidirectional_header_size] = std::byte{6};
    CHECK_FALSE(decode_rpc_response(encoded).has_value());
}

TEST_CASE("RPC response frame: status byte at the highest enumerator decodes", "[wire][rpc]")
{
    bidirectional_header hdr{.source         = endpoint_source_type::procedure,
                             .sequence       = 8,
                             .topic_hash     = 0xAAAA,
                             .type_hash_1    = 0xCCCC,
                             .type_hash_2    = 0xBBBB,
                             .correlation_id = 42};

    // rpc_response_orphan = 20 is the highest valid status — the boundary decodes.
    auto encoded = encode_rpc_response(hdr, rpc_status::rpc_response_orphan, {});
    auto decoded = decode_rpc_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->status == rpc_status::rpc_response_orphan);
}

TEST_CASE("RPC request frame: empty payload round-trip", "[wire][rpc]")
{
    bidirectional_header hdr{.source         = endpoint_source_type::caller,
                             .sequence       = 5,
                             .topic_hash     = 0xAAAA,
                             .type_hash_1    = 0xBBBB,
                             .type_hash_2    = 0xCCCC,
                             .correlation_id = 99};

    auto encoded = encode_rpc_request(hdr, {});
    auto decoded = decode_rpc_request(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->param_data.empty());
}

TEST_CASE("RPC request frame: decode fails below the bidirectional header minimum", "[wire][rpc]")
{
    std::array<std::byte, bidirectional_header_size - 1> short_buf{};
    CHECK_FALSE(decode_rpc_request(short_buf).has_value());
}

TEST_CASE("RPC response frame: decode fails with header but zero inner bytes", "[wire][rpc]")
{
    bidirectional_header hdr{.source         = endpoint_source_type::procedure,
                             .sequence       = 9,
                             .topic_hash     = 0xAAAA,
                             .type_hash_1    = 0xCCCC,
                             .type_hash_2    = 0xBBBB,
                             .correlation_id = 42};

    // A bidirectional frame with an empty body has no room for the status byte.
    auto encoded = encode_bidirectional(hdr, {});
    CHECK(encoded.size() == bidirectional_header_size);
    CHECK_FALSE(decode_rpc_response(encoded).has_value());
}

// --- Handshake codec tests ---

namespace {

std::array<std::byte, 16> id_filled(std::uint8_t value)
{
    std::array<std::byte, 16> id{};
    id.fill(std::byte{value});
    return id;
}

std::array<std::byte, 16> id_mixed_high_bit()
{
    std::array<std::byte, 16> id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = std::byte{static_cast<std::uint8_t>(i % 2 == 0 ? 0x80 : 0x7F)};
    return id;
}

std::array<std::byte, 16> id_distinct()
{
    std::array<std::byte, 16> id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = std::byte{static_cast<std::uint8_t>(0x10 + i)};
    return id;
}

std::array<std::byte, 8> key_id_seed()
{
    std::array<std::byte, 8> k{};
    for(std::size_t i = 0; i < k.size(); ++i)
        k[i] = static_cast<std::byte>(0x60 + i);
    return k;
}

std::array<std::byte, 16> nonce_seed()
{
    std::array<std::byte, 16> n{};
    for(std::size_t i = 0; i < n.size(); ++i)
        n[i] = static_cast<std::byte>(0x70 + i);
    return n;
}

std::array<std::byte, k_handshake_proof_len> proof_seed(std::uint8_t base = 0x90)
{
    std::array<std::byte, k_handshake_proof_len> pr{};
    for(std::size_t i = 0; i < pr.size(); ++i)
        pr[i] = static_cast<std::byte>(base + i);
    return pr;
}

handshake_request make_request(const std::array<std::byte, 16> &id)
{
    return handshake_request{.id                       = id,
                             .version_major            = 0x11,
                             .version_minor            = 0x22,
                             .compatible_version_major = 0x33,
                             .compatible_version_minor = 0x44,
                             .protocol_version         = 0x55,
                             .fingerprint              = 0x0123456789ABCDEFull,
                             .key_id                   = key_id_seed(),
                             .own_nonce                = nonce_seed(),
                             .cipher_offer             = cipher_offer_bits::chacha20_poly1305 |
                                     cipher_offer_bits::aes_256_gcm,
                             .chosen_cipher = cipher_offer_bits::chacha20_poly1305,
                             .proof         = proof_seed()};
}

handshake_response make_response(const std::array<std::byte, 16> &id, handshake_status status)
{
    return handshake_response{.id                       = id,
                              .version_major            = 0x11,
                              .version_minor            = 0x22,
                              .compatible_version_major = 0x33,
                              .compatible_version_minor = 0x44,
                              .protocol_version         = 0x55,
                              .fingerprint              = 0xFEDCBA9876543210ull,
                              .key_id                   = key_id_seed(),
                              .own_nonce                = nonce_seed(),
                              .cipher_offer             = cipher_offer_bits::chacha20_poly1305 |
                                      cipher_offer_bits::aes_256_gcm,
                              .chosen_cipher = cipher_offer_bits::aes_256_gcm,
                              .proof         = proof_seed(0xC1),
                              .status        = status};
}

void check_request_equal(const handshake_request &a, const handshake_request &b)
{
    CHECK(a.id == b.id);
    CHECK(a.version_major == b.version_major);
    CHECK(a.version_minor == b.version_minor);
    CHECK(a.compatible_version_major == b.compatible_version_major);
    CHECK(a.compatible_version_minor == b.compatible_version_minor);
    CHECK(a.protocol_version == b.protocol_version);
    CHECK(a.fingerprint == b.fingerprint);
    CHECK(a.key_id == b.key_id);
    CHECK(a.own_nonce == b.own_nonce);
    CHECK(a.cipher_offer == b.cipher_offer);
    CHECK(a.chosen_cipher == b.chosen_cipher);
    CHECK(a.proof == b.proof);
}

}

TEST_CASE("Handshake request: round-trip across the id field space", "[wire][handshake]")
{
    for(const auto &id : {id_distinct(), id_filled(0x00), id_filled(0xFF), id_mixed_high_bit()})
    {
        auto req     = make_request(id);
        auto decoded = decode_handshake_request(encode_handshake_request(req));
        REQUIRE(decoded.has_value());
        check_request_equal(*decoded, req);
        CHECK(decoded->id == id); // opaque memcpy: id bytes survive verbatim, no swap
    }
}

TEST_CASE("Handshake response: round-trip across id field space and all four statuses",
          "[wire][handshake]")
{
    const handshake_status statuses[] = {
            handshake_status::accepted, handshake_status::version_incompatible,
            handshake_status::identity_conflict, handshake_status::rejected_unknown};

    for(const auto &id : {id_distinct(), id_filled(0x00), id_filled(0xFF), id_mixed_high_bit()})
        for(auto status : statuses)
        {
            auto resp    = make_response(id, status);
            auto decoded = decode_handshake_response(encode_handshake_response(resp));
            REQUIRE(decoded.has_value());
            CHECK(decoded->id == id);
            CHECK(decoded->version_major == resp.version_major);
            CHECK(decoded->version_minor == resp.version_minor);
            CHECK(decoded->compatible_version_major == resp.compatible_version_major);
            CHECK(decoded->compatible_version_minor == resp.compatible_version_minor);
            CHECK(decoded->protocol_version == resp.protocol_version);
            CHECK(decoded->fingerprint == resp.fingerprint);
            CHECK(decoded->status == status);
        }
}

TEST_CASE("Handshake codec: encoded wire-size pins", "[wire][handshake]")
{
    // id(16) + 5 single-byte fields + fingerprint(8) + the attach region
    // key_id(8)+own_nonce(16)+cipher_offer(1)+chosen(1)+proof(32) = 87; +status(1) = 88.
    CHECK(encode_handshake_request(make_request(id_distinct())).size() == 87);
    CHECK(encode_handshake_response(make_response(id_distinct(), handshake_status::accepted))
                  .size() == 88);
}

TEST_CASE("Handshake request: every length below the fixed size returns nullopt",
          "[wire][handshake]")
{
    for(std::size_t len = 0; len < handshake_request_size; ++len)
    {
        std::vector<std::byte> payload(len);
        CHECK_FALSE(decode_handshake_request(payload).has_value());
    }
    std::vector<std::byte> exact(handshake_request_size);
    CHECK(decode_handshake_request(exact).has_value());
}

TEST_CASE("Handshake response: every length below the fixed size returns nullopt",
          "[wire][handshake]")
{
    auto valid =
            encode_handshake_response(make_response(id_distinct(), handshake_status::accepted));
    for(std::size_t len = 0; len < handshake_response_size; ++len)
    {
        std::vector<std::byte> payload(valid.begin(), valid.begin() + len);
        CHECK_FALSE(decode_handshake_response(payload).has_value());
    }
    CHECK(decode_handshake_response(valid).has_value());
}

TEST_CASE("Handshake response: status cutoff rejects 0x00 and every byte 0x06..0xFF",
          "[wire][handshake]")
{
    auto encoded =
            encode_handshake_response(make_response(id_distinct(), handshake_status::accepted));

    // The status byte is the LAST byte, after the appended attach region, at offset
    // handshake_request_size (the response is the request plus the trailing status).
    encoded[handshake_request_size] = std::byte{0x00};
    CHECK_FALSE(decode_handshake_response(encoded).has_value());

    for(int b = 0x06; b <= 0xFF; ++b)
    {
        encoded[handshake_request_size] = std::byte{static_cast<std::uint8_t>(b)};
        CHECK_FALSE(decode_handshake_response(encoded).has_value());
    }
}

TEST_CASE("Handshake response: each defined status byte 0x01..0x05 decodes to its enumerator",
          "[wire][handshake]")
{
    auto encoded =
            encode_handshake_response(make_response(id_distinct(), handshake_status::accepted));
    const std::pair<std::uint8_t, handshake_status> defined[] = {
            {0x01, handshake_status::accepted},
            {0x02, handshake_status::version_incompatible},
            {0x03, handshake_status::identity_conflict},
            {0x04, handshake_status::rejected_unknown},
            {0x05, handshake_status::unauthorized}};

    for(auto [byte, status] : defined)
    {
        encoded[handshake_request_size] = std::byte{byte};
        auto decoded                    = decode_handshake_response(encoded);
        REQUIRE(decoded.has_value());
        CHECK(decoded->status == status);
    }
}

TEST_CASE("Handshake encode-into: byte-identical to the allocating encoder", "[wire][handshake]")
{
    auto                   req            = make_request(id_mixed_high_bit());
    auto                   req_allocating = encode_handshake_request(req);
    std::vector<std::byte> req_reused;
    encode_handshake_request_into(req_reused, req);
    CHECK(req_reused == req_allocating);

    auto resp            = make_response(id_mixed_high_bit(), handshake_status::identity_conflict);
    auto resp_allocating = encode_handshake_response(resp);
    std::vector<std::byte> resp_reused;
    encode_handshake_response_into(resp_reused, resp);
    CHECK(resp_reused == resp_allocating);
}
