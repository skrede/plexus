#include "plexus/wire/varint.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

using namespace plexus::wire;

namespace {

// Decode a varint from the start of a freshly-encoded buffer, returning both the
// value and the consumed count so a case can assert the advance discipline.
struct decode_result
{
    std::optional<std::uint64_t> value;
    std::size_t                  consumed;
};

decode_result round_trip(std::uint64_t v)
{
    std::vector<std::byte> out;
    write_varint(out, v);
    std::size_t consumed = 0;
    auto decoded = read_varint(out, consumed);
    return {decoded, consumed};
}

}

TEST_CASE("varint round-trips the full u64 value space", "[wire][varint]")
{
    for(std::uint64_t v : {std::uint64_t{0}, std::uint64_t{1}, std::uint64_t{0x7F},
                           std::uint64_t{0x80}, std::uint64_t{300},
                           std::uint64_t{0xDEADBEEF},
                           std::numeric_limits<std::uint64_t>::max()})
    {
        const auto r = round_trip(v);
        REQUIRE(r.value.has_value());
        CHECK(*r.value == v);
        CHECK(r.consumed > 0);
    }
}

TEST_CASE("varint emits one byte per 7 value bits", "[wire][varint]")
{
    std::vector<std::byte> out;
    write_varint(out, 0);
    CHECK(out.size() == 1);

    out.clear();
    write_varint(out, 0x7F);
    CHECK(out.size() == 1);

    out.clear();
    write_varint(out, 0x80);
    CHECK(out.size() == 2);

    out.clear();
    write_varint(out, std::numeric_limits<std::uint64_t>::max());
    CHECK(out.size() == 10);
}

TEST_CASE("varint write appends without clearing the caller's buffer", "[wire][varint]")
{
    std::vector<std::byte> out;
    out.push_back(std::byte{0xAB});
    write_varint(out, 1);
    REQUIRE(out.size() == 2);
    CHECK(out[0] == std::byte{0xAB});
}

TEST_CASE("varint decode rejects a truncated continuation without advancing", "[wire][varint]")
{
    // A single byte with the continuation bit set but no following byte.
    std::vector<std::byte> buf{std::byte{0x80}};
    std::size_t consumed = 0;
    const auto decoded = read_varint(buf, consumed);
    CHECK_FALSE(decoded.has_value());
    CHECK(consumed == 0);
}

TEST_CASE("varint decode rejects an over-long encoding past 10 bytes", "[wire][varint]")
{
    // Eleven bytes, every one carrying the continuation bit: exceeds the u64 cap.
    std::vector<std::byte> buf(11, std::byte{0x80});
    std::size_t consumed = 0;
    const auto decoded = read_varint(buf, consumed);
    CHECK_FALSE(decoded.has_value());
    CHECK(consumed == 0);
}

TEST_CASE("varint decode rejects a 10th byte that overflows u64", "[wire][varint]")
{
    // Nine continuation bytes carrying zero payload, then a terminator whose bits
    // would shift past bit 63. 10 bytes encode at most 70 bits; the top byte may
    // carry only the single bit 63, so a 10th byte > 0x01 overflows.
    std::vector<std::byte> buf(9, std::byte{0x80});
    buf.push_back(std::byte{0x02}); // terminator, but bits beyond bit 63
    std::size_t consumed = 0;
    const auto decoded = read_varint(buf, consumed);
    CHECK_FALSE(decoded.has_value());
    CHECK(consumed == 0);
}

TEST_CASE("varint decode advances consumed only by the bytes it read", "[wire][varint]")
{
    std::vector<std::byte> out;
    write_varint(out, 0x80); // two bytes
    out.push_back(std::byte{0xCD}); // a trailing byte the decode must not touch
    std::size_t consumed = 0;
    const auto decoded = read_varint(out, consumed);
    REQUIRE(decoded.has_value());
    CHECK(*decoded == 0x80);
    CHECK(consumed == 2);
}

TEST_CASE("varint decode honors a non-zero starting consumed offset", "[wire][varint]")
{
    std::vector<std::byte> out;
    out.push_back(std::byte{0x11});
    out.push_back(std::byte{0x22});
    write_varint(out, 300);
    std::size_t consumed = 2;
    const auto decoded = read_varint(out, consumed);
    REQUIRE(decoded.has_value());
    CHECK(*decoded == 300);
    CHECK(consumed > 2);
}

TEST_CASE("varint decode of an empty span returns nullopt", "[wire][varint]")
{
    std::span<const std::byte> empty;
    std::size_t consumed = 0;
    const auto decoded = read_varint(empty, consumed);
    CHECK_FALSE(decoded.has_value());
    CHECK(consumed == 0);
}
