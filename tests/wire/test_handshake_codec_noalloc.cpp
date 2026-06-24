#include "plexus/wire/handshake.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>

using namespace plexus::wire;

// The encode-into NOALLOC gate lives in its OWN executable: support/alloc_counter.h
// replaces global operator new/delete and may be included in only one TU per
// program, so it cannot share test_wire_codec.cpp. Warm the reused vectors to
// steady size, snapshot the alloc counter, then prove a K-iteration loop reusing
// the same buffers allocates nothing.

namespace {

handshake_request sample_request()
{
    std::array<std::byte, 16> id{};
    id.fill(std::byte{0xA5});
    return handshake_request{.id                       = id,
                             .version_major            = 1,
                             .version_minor            = 2,
                             .compatible_version_major = 3,
                             .compatible_version_minor = 4,
                             .protocol_version         = 5,
                             .fingerprint              = 0xA5A5A5A5A5A5A5A5ull,
                             .key_id                   = {},
                             .own_nonce                = {},
                             .cipher_offer             = 0,
                             .chosen_cipher            = 0,
                             .proof                    = {}};
}

handshake_response sample_response()
{
    std::array<std::byte, 16> id{};
    id.fill(std::byte{0x5A});
    return handshake_response{.id                       = id,
                              .version_major            = 1,
                              .version_minor            = 2,
                              .compatible_version_major = 3,
                              .compatible_version_minor = 4,
                              .protocol_version         = 5,
                              .fingerprint              = 0x5A5A5A5A5A5A5A5Aull,
                              .key_id                   = {},
                              .own_nonce                = {},
                              .cipher_offer             = 0,
                              .chosen_cipher            = 0,
                              .proof                    = {},
                              .status                   = handshake_status::accepted};
}

}

TEST_CASE("Handshake encode-into: zero allocation across a steady-state loop", "[wire][handshake][noalloc]")
{
    constexpr int K = 1024;
    const auto req  = sample_request();
    const auto resp = sample_response();

    std::vector<std::byte> req_buf;
    std::vector<std::byte> resp_buf;

    // Warm-up: grow the reused buffers to their fixed 87/88-byte capacity.
    encode_handshake_request_into(req_buf, req);
    encode_handshake_response_into(resp_buf, resp);

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
    {
        encode_handshake_request_into(req_buf, req);
        encode_handshake_response_into(resp_buf, resp);
    }
    const auto after = plexus::testing::alloc_count();

    REQUIRE(after - before == 0); // zero allocation across the steady-state loop
}
