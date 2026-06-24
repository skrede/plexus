#include "test_wire_codec_handshake_common.h"

using namespace wire_codec_handshake_fixture;

// --- Handshake codec tests ---

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

TEST_CASE("Handshake response: round-trip across id field space and all four statuses", "[wire][handshake]")
{
    const handshake_status statuses[] = {handshake_status::accepted, handshake_status::version_incompatible, handshake_status::identity_conflict, handshake_status::rejected_unknown};

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
    CHECK(encode_handshake_response(make_response(id_distinct(), handshake_status::accepted)).size() == 88);
}

TEST_CASE("Handshake request: every length below the fixed size returns nullopt", "[wire][handshake]")
{
    for(std::size_t len = 0; len < handshake_request_size; ++len)
    {
        std::vector<std::byte> payload(len);
        CHECK_FALSE(decode_handshake_request(payload).has_value());
    }
    std::vector<std::byte> exact(handshake_request_size);
    CHECK(decode_handshake_request(exact).has_value());
}

TEST_CASE("Handshake response: every length below the fixed size returns nullopt", "[wire][handshake]")
{
    auto valid = encode_handshake_response(make_response(id_distinct(), handshake_status::accepted));
    for(std::size_t len = 0; len < handshake_response_size; ++len)
    {
        std::vector<std::byte> payload(valid.begin(), valid.begin() + len);
        CHECK_FALSE(decode_handshake_response(payload).has_value());
    }
    CHECK(decode_handshake_response(valid).has_value());
}

TEST_CASE("Handshake response: status cutoff rejects 0x00 and every byte 0x06..0xFF", "[wire][handshake]")
{
    auto encoded = encode_handshake_response(make_response(id_distinct(), handshake_status::accepted));

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

TEST_CASE("Handshake response: each defined status byte 0x01..0x05 decodes to its enumerator", "[wire][handshake]")
{
    auto                                            encoded   = encode_handshake_response(make_response(id_distinct(), handshake_status::accepted));
    const std::pair<std::uint8_t, handshake_status> defined[] = {{0x01, handshake_status::accepted},
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

    auto                   resp            = make_response(id_mixed_high_bit(), handshake_status::identity_conflict);
    auto                   resp_allocating = encode_handshake_response(resp);
    std::vector<std::byte> resp_reused;
    encode_handshake_response_into(resp_reused, resp);
    CHECK(resp_reused == resp_allocating);
}
