#include "plexus/wire/fetch_latched.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>

using namespace plexus::wire;

TEST_CASE("fetch_latched_request round-trips topic_hash and max_samples", "[wire][fetch_latched]")
{
    const fetch_latched_request req{.topic_hash = 0xDEADBEEFCAFEF00DULL, .max_samples = 42};

    auto bytes = encode_fetch_latched_request(req);
    REQUIRE(bytes.size() == detail::fetch_latched_request_size); // exactly 12 bytes
    REQUIRE(bytes.size() == 12);

    auto decoded = decode_fetch_latched_request(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->topic_hash == req.topic_hash);
    REQUIRE(decoded->max_samples == req.max_samples);
}

TEST_CASE("a truncated fetch_latched_request decodes to nullopt", "[wire][fetch_latched]")
{
    auto bytes = encode_fetch_latched_request({.topic_hash = 1, .max_samples = 1});

    // A payload shorter than the fixed 12 bytes is rejected before any read — the
    // bounds-safe gate for a NEW untrusted-input decoder (no over-read).
    auto truncated = std::span<const std::byte>{bytes}.subspan(0, 11);
    REQUIRE_FALSE(decode_fetch_latched_request(truncated).has_value());

    REQUIRE_FALSE(decode_fetch_latched_request(std::span<const std::byte>{}).has_value());
}
