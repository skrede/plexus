// Parameterized bounds + overflow exercise for the bounds-only length-prefix
// helper. The decoder is the primary untrusted-input gate: the table below
// pins the four ordered checks (offset, prefix-fits, multiply-overflow,
// payload-fits) across every supported prefix width.

#include "plexus/wire/length_prefixed.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <climits>
#include <limits>

using namespace plexus::wire;

namespace {

// Big-endian length-prefix builders shared across u8 / u16 / u32 / u64 cases.
template<typename UIntT>
std::vector<std::byte> pack_prefix_be(UIntT length)
{
    std::vector<std::byte> out(sizeof(UIntT));
    for(std::size_t i = 0; i < sizeof(UIntT); ++i)
        out[sizeof(UIntT) - 1 - i] = static_cast<std::byte>((length >> (i * 8)) & 0xFF);
    return out;
}

template<typename UIntT>
std::vector<std::byte> with_payload(UIntT length, std::size_t payload_bytes,
                                    std::byte fill = std::byte{0xAB})
{
    auto out = pack_prefix_be<UIntT>(length);
    out.insert(out.end(), payload_bytes, fill);
    return out;
}

template<typename UIntT>
void expect_some(std::span<const std::byte> input, std::size_t start_consumed,
                 std::size_t element_size, std::size_t expect_consumed_after,
                 std::size_t expect_payload_bytes)
{
    std::size_t consumed = start_consumed;
    auto        result   = read_length_prefixed<UIntT>(input, consumed, element_size);
    REQUIRE(result.has_value());
    CHECK(consumed == expect_consumed_after);
    CHECK(result->size() == expect_payload_bytes);
}

template<typename UIntT>
void expect_none(std::span<const std::byte> input, std::size_t start_consumed,
                 std::size_t element_size)
{
    std::size_t consumed = start_consumed;
    auto        result   = read_length_prefixed<UIntT>(input, consumed, element_size);
    CHECK_FALSE(result.has_value());
    CHECK(consumed == start_consumed);
}

}

// u16 table. Eleven sections covering bounds, multiply-overflow, and
// start_consumed offsetting.
TEST_CASE("read_length_prefixed<uint16_t> bounds + overflow table", "[wire][length_prefixed]")
{
    SECTION("empty input + non-zero consumed")
    {
        expect_none<std::uint16_t>(std::vector<std::byte>{}, 4, 1);
    }
    SECTION("consumed greater than data.size")
    {
        expect_none<std::uint16_t>(std::vector<std::byte>(4), 10, 1);
    }
    SECTION("buffer too short for prefix")
    {
        expect_none<std::uint16_t>(std::vector<std::byte>(1), 0, 1);
    }
    SECTION("prefix only, length=0")
    {
        expect_some<std::uint16_t>(with_payload<std::uint16_t>(0, 0), 0, 1, 2, 0);
    }
    SECTION("prefix + 1-byte payload")
    {
        expect_some<std::uint16_t>(with_payload<std::uint16_t>(1, 1), 0, 1, 3, 1);
    }
    SECTION("prefix + full-remaining payload")
    {
        expect_some<std::uint16_t>(with_payload<std::uint16_t>(8, 8), 0, 1, 10, 8);
    }
    SECTION("prefix declares length larger than remaining")
    {
        expect_none<std::uint16_t>(with_payload<std::uint16_t>(16, 4), 0, 1);
    }
    SECTION("element_size=2 fits")
    {
        expect_some<std::uint16_t>(with_payload<std::uint16_t>(3, 6), 0, 2, 8, 6);
    }
    SECTION("element_size=2 overflow against remaining")
    {
        expect_none<std::uint16_t>(with_payload<std::uint16_t>(3, 4), 0, 2);
    }
    SECTION("non-zero start_consumed, prefix at offset")
    {
        std::vector<std::byte> v(2, std::byte{0xCC});
        auto                   p = with_payload<std::uint16_t>(2, 2);
        v.insert(v.end(), p.begin(), p.end());
        expect_some<std::uint16_t>(v, 2, 1, 6, 2);
    }
    SECTION("element_size=0 short-circuits payload to zero")
    {
        expect_some<std::uint16_t>(with_payload<std::uint16_t>(0, 0), 0, 0, 2, 0);
    }
}

TEST_CASE("read_length_prefixed<uint8_t> bounds table", "[wire][length_prefixed]")
{
    SECTION("u8 prefix length=0")
    {
        expect_some<std::uint8_t>(with_payload<std::uint8_t>(0, 0), 0, 1, 1, 0);
    }
    SECTION("u8 prefix length=5, full payload")
    {
        expect_some<std::uint8_t>(with_payload<std::uint8_t>(5, 5), 0, 1, 6, 5);
    }
    SECTION("u8 prefix length=5, short payload")
    {
        expect_none<std::uint8_t>(with_payload<std::uint8_t>(5, 3), 0, 1);
    }
}

TEST_CASE("read_length_prefixed<uint32_t> bounds + overflow table", "[wire][length_prefixed]")
{
    SECTION("u32 prefix length=0")
    {
        expect_some<std::uint32_t>(with_payload<std::uint32_t>(0, 0), 0, 1, 4, 0);
    }
    SECTION("u32 prefix length=8, full payload")
    {
        expect_some<std::uint32_t>(with_payload<std::uint32_t>(8, 8), 0, 1, 12, 8);
    }
    SECTION("u32 prefix length=8, missing 1 byte")
    {
        expect_none<std::uint32_t>(with_payload<std::uint32_t>(8, 7), 0, 1);
    }
    SECTION("u32 element_size=4, length x element overflows remaining")
    {
        expect_none<std::uint32_t>(with_payload<std::uint32_t>(3, 8), 0, 4);
    }
}

TEST_CASE("read_length_prefixed<uint64_t> bounds + overflow table", "[wire][length_prefixed]")
{
    SECTION("u64 prefix length=0")
    {
        expect_some<std::uint64_t>(with_payload<std::uint64_t>(0, 0), 0, 1, 8, 0);
    }
    SECTION("u64 prefix length=4, full payload")
    {
        expect_some<std::uint64_t>(with_payload<std::uint64_t>(4, 4), 0, 1, 12, 4);
    }
    SECTION("u64 element_size=4 with length x element wraps size_t")
    {
        // SIZE_MAX/4 + 1 as the u64 length: length * 4 wraps size_t on a 64-bit
        // host (size_t = uint64_t). Helper must reject via the pre-multiply
        // overflow guard. Payload bytes are immaterial (rejection happens
        // before the remaining-bytes check).
        const auto sentinel =
                static_cast<std::uint64_t>((std::numeric_limits<std::size_t>::max() / 4u) + 1u);
        expect_none<std::uint64_t>(pack_prefix_be<std::uint64_t>(sentinel), 0, 4);
    }
}
