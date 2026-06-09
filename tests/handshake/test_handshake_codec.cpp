// The widened handshake-frame codec oracle: the same-host fingerprint rides the
// frame as an append-only fixed-width field. This focused oracle re-pins the
// round-trip across the fingerprint value space, the bounds-safety at the new
// size, and the protocol-version bump that gates the layout change — selectable
// under `ctest -R handshake`. The exhaustive id/status/cutoff matrix stays in the
// wire-codec suite; this one owns the fingerprint-specific coverage.

#include "plexus/wire/handshake.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus::wire;

namespace {

std::array<std::byte, 16> id_seed(std::uint8_t v)
{
    std::array<std::byte, 16> id{};
    id.fill(std::byte{v});
    return id;
}

handshake_request request_with(std::uint64_t fingerprint)
{
    return handshake_request{
            .id                       = id_seed(0xA5),
            .version_major            = 1,
            .version_minor            = 2,
            .compatible_version_major = 3,
            .compatible_version_minor = 4,
            .protocol_version         = k_protocol_version,
            .fingerprint              = fingerprint};
}

handshake_response response_with(std::uint64_t fingerprint, handshake_status status)
{
    return handshake_response{
            .id                       = id_seed(0x5A),
            .version_major            = 1,
            .version_minor            = 2,
            .compatible_version_major = 3,
            .compatible_version_minor = 4,
            .protocol_version         = k_protocol_version,
            .fingerprint              = fingerprint,
            .status                   = status};
}

}

TEST_CASE("handshake codec: the request round-trips the fingerprint byte-equal", "[handshake][codec][fingerprint]")
{
    for(std::uint64_t fp : {std::uint64_t{0}, std::uint64_t{1}, std::uint64_t{0xDEADBEEFCAFEF00Dull},
                            ~std::uint64_t{0}})
    {
        auto req = request_with(fp);
        auto decoded = decode_handshake_request(encode_handshake_request(req));
        REQUIRE(decoded.has_value());
        CHECK(decoded->fingerprint == fp);
        CHECK(decoded->id == req.id);
        CHECK(decoded->protocol_version == req.protocol_version);
    }
}

TEST_CASE("handshake codec: the response round-trips the fingerprint byte-equal", "[handshake][codec][fingerprint]")
{
    for(std::uint64_t fp : {std::uint64_t{0}, std::uint64_t{0x0123456789ABCDEFull}, ~std::uint64_t{0}})
    {
        auto resp = response_with(fp, handshake_status::accepted);
        auto decoded = decode_handshake_response(encode_handshake_response(resp));
        REQUIRE(decoded.has_value());
        CHECK(decoded->fingerprint == fp);
        CHECK(decoded->status == handshake_status::accepted);
    }
}

TEST_CASE("handshake codec: a zero fingerprint round-trips as the null (not-same-host) value",
          "[handshake][codec][fingerprint]")
{
    auto decoded = decode_handshake_request(encode_handshake_request(request_with(0)));
    REQUIRE(decoded.has_value());
    CHECK(decoded->fingerprint == 0u); // the conservative not-same-host signal
}

TEST_CASE("handshake codec: the appended fingerprint widens the wire-size constants", "[handshake][codec][fingerprint]")
{
    // id(16) + 5 single-byte fields(5) + fingerprint(8) = 29; response adds status(1) = 30.
    static_assert(handshake_request_size == 29);
    static_assert(handshake_response_size == 30);
    static_assert(handshake_response_size == handshake_request_size + 1);
    CHECK(encode_handshake_request(request_with(1)).size() == handshake_request_size);
    CHECK(encode_handshake_response(response_with(1, handshake_status::accepted)).size() ==
          handshake_response_size);
}

TEST_CASE("handshake codec: a payload one byte below the widened size returns nullopt", "[handshake][codec][fingerprint]")
{
    auto req = encode_handshake_request(request_with(7));
    std::vector<std::byte> shortened(req.begin(), req.end() - 1);
    CHECK_FALSE(decode_handshake_request(shortened).has_value());
    CHECK(decode_handshake_request(req).has_value());

    auto resp = encode_handshake_response(response_with(7, handshake_status::accepted));
    std::vector<std::byte> resp_short(resp.begin(), resp.end() - 1);
    CHECK_FALSE(decode_handshake_response(resp_short).has_value());
    CHECK(decode_handshake_response(resp).has_value());
}

TEST_CASE("handshake codec: the status cutoff still rejects an undefined byte under the widened frame",
          "[handshake][codec][fingerprint]")
{
    auto encoded = encode_handshake_response(response_with(7, handshake_status::accepted));
    encoded[handshake_request_size] = std::byte{0x00}; // the status byte sits AFTER the fingerprint
    CHECK_FALSE(decode_handshake_response(encoded).has_value());
    for(int b = 0x05; b <= 0xFF; ++b)
    {
        encoded[handshake_request_size] = std::byte{static_cast<std::uint8_t>(b)};
        CHECK_FALSE(decode_handshake_response(encoded).has_value());
    }
}

TEST_CASE("handshake codec: the protocol version gates the layout change", "[handshake][codec][fingerprint]")
{
    static_assert(k_protocol_version == 4); // the appended field is a layout change a skewed peer must reject
    CHECK(k_protocol_version == 4);
}
