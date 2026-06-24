#include "test_wire_codec_common.h"

// --- RPC request/response codec tests ---

TEST_CASE("RPC request frame: round-trip encode/decode", "[wire][rpc]")
{
    bidirectional_header     hdr{.source = endpoint_source_type::caller, .sequence = 1, .topic_hash = 0xAAAA, .type_hash_1 = 0xBBBB, .type_hash_2 = 0xCCCC, .correlation_id = 42};
    std::array<std::byte, 4> param_data{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}};

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
    bidirectional_header     hdr{.source = endpoint_source_type::procedure, .sequence = 2, .topic_hash = 0xAAAA, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 42};
    std::array<std::byte, 4> return_data{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}, std::byte{0x40}};

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
    bidirectional_header hdr{.source = endpoint_source_type::procedure, .sequence = 3, .topic_hash = 0xAAAA, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 42};

    auto encoded = encode_rpc_response(hdr, rpc_status::no_handler, {});
    auto decoded = decode_rpc_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->status == rpc_status::no_handler);
    CHECK(decoded->return_data.empty());
}

TEST_CASE("RPC response frame: zero-alloc _into matches the allocating encode", "[wire][rpc]")
{
    bidirectional_header     hdr{.source = endpoint_source_type::procedure, .sequence = 11, .topic_hash = 0x1234, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 7};
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

TEST_CASE("RPC response frame: status byte above the highest enumerator returns nullopt", "[wire][rpc]")
{
    bidirectional_header     hdr{.source = endpoint_source_type::procedure, .sequence = 4, .topic_hash = 0xAAAA, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 42};
    std::array<std::byte, 4> return_data{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}, std::byte{0x40}};

    auto encoded                       = encode_rpc_response(hdr, rpc_status::success, return_data);
    encoded[bidirectional_header_size] = std::byte{0xFF};
    auto decoded                       = decode_rpc_response(encoded);

    CHECK_FALSE(decoded.has_value());
}

TEST_CASE("RPC response frame: in-range reserved-gap status byte returns nullopt", "[wire][rpc]")
{
    bidirectional_header     hdr{.source = endpoint_source_type::procedure, .sequence = 6, .topic_hash = 0xAAAA, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 42};
    std::array<std::byte, 2> return_data{std::byte{0x10}, std::byte{0x20}};

    // 6 sits below the highest enumerator (20) but is a reserved gap with no
    // defined rpc_status — the decoder must reject it, never yield an undefined value.
    auto encoded                       = encode_rpc_response(hdr, rpc_status::success, return_data);
    encoded[bidirectional_header_size] = std::byte{6};
    CHECK_FALSE(decode_rpc_response(encoded).has_value());
}

TEST_CASE("RPC response frame: status byte at the highest enumerator decodes", "[wire][rpc]")
{
    bidirectional_header hdr{.source = endpoint_source_type::procedure, .sequence = 8, .topic_hash = 0xAAAA, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 42};

    // rpc_response_orphan = 20 is the highest valid status — the boundary decodes.
    auto encoded = encode_rpc_response(hdr, rpc_status::rpc_response_orphan, {});
    auto decoded = decode_rpc_response(encoded);

    REQUIRE(decoded.has_value());
    CHECK(decoded->status == rpc_status::rpc_response_orphan);
}

TEST_CASE("RPC request frame: empty payload round-trip", "[wire][rpc]")
{
    bidirectional_header hdr{.source = endpoint_source_type::caller, .sequence = 5, .topic_hash = 0xAAAA, .type_hash_1 = 0xBBBB, .type_hash_2 = 0xCCCC, .correlation_id = 99};

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
    bidirectional_header hdr{.source = endpoint_source_type::procedure, .sequence = 9, .topic_hash = 0xAAAA, .type_hash_1 = 0xCCCC, .type_hash_2 = 0xBBBB, .correlation_id = 42};

    // A bidirectional frame with an empty body has no room for the status byte.
    auto encoded = encode_bidirectional(hdr, {});
    CHECK(encoded.size() == bidirectional_header_size);
    CHECK_FALSE(decode_rpc_response(encoded).has_value());
}
