#include "plexus/wire/crc32c.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace plexus::wire;

namespace {

std::span<const std::byte> as_bytes(std::string_view s) noexcept
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

}

TEST_CASE("crc32c matches the canonical check vector", "[wire][crc32c]")
{
    // The Castagnoli check value (CRC-32C of the ASCII "123456789"), the vector
    // every CRC-32C implementation publishes (RFC 3720 / Koopman).
    REQUIRE(crc32c(as_bytes("123456789")) == 0xE3069283u);
}

TEST_CASE("crc32c of an empty span is zero", "[wire][crc32c]")
{
    REQUIRE(crc32c({}) == 0x00000000u);
}

TEST_CASE("crc32c is incremental over contiguous chunks", "[wire][crc32c]")
{
    const std::string whole = "the quick brown fox jumps over the lazy dog";
    const auto        bytes = as_bytes(whole);

    const std::uint32_t one_shot = crc32c(bytes);

    for(std::size_t split = 0; split <= whole.size(); ++split)
    {
        const std::uint32_t seed    = crc32c(bytes.first(split));
        const std::uint32_t chained = crc32c(bytes.subspan(split), seed);
        REQUIRE(chained == one_shot);
    }
}
