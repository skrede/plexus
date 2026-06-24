#include "plexus/wire/cursor.h"
#include "plexus/wire/varint.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus::wire;

TEST_CASE("wire::writer then wire::reader round-trips a u8/u64/u64 field list", "[wire][cursor]")
{
    std::vector<std::byte> buf(sizeof(std::uint8_t) + 2 * sizeof(std::uint64_t));
    writer                 w{buf};
    w.u8(0xA5);
    w.u64(0xDEADBEEFCAFEBABEull);
    w.u64(0x0102030405060708ull);
    REQUIRE(w.offset() == buf.size());

    reader r{buf};
    REQUIRE(r.u8() == 0xA5);
    REQUIRE(r.u64() == 0xDEADBEEFCAFEBABEull);
    REQUIRE(r.u64() == 0x0102030405060708ull);
    REQUIRE(r.ok());
    REQUIRE(r.remaining() == 0);
}

TEST_CASE("wire::writer/reader round-trip an opaque blob via bytes()", "[wire][cursor]")
{
    std::array<std::byte, 4> blob{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    std::vector<std::byte>   buf(sizeof(std::uint16_t) + blob.size());
    writer                   w{buf};
    w.u16(0x1234);
    w.bytes(blob);

    reader r{buf};
    REQUIRE(r.u16() == 0x1234);
    auto view = r.bytes(blob.size());
    REQUIRE(r.ok());
    REQUIRE(std::equal(view.begin(), view.end(), blob.begin()));
}

TEST_CASE("wire::reader one byte short of a u64 fails closed without over-reading", "[wire][cursor]")
{
    std::array<std::byte, 7> short_buf{};
    reader                   r{short_buf};
    auto                     v = r.u64();
    REQUIRE(v == 0);
    REQUIRE_FALSE(r.ok());
    // A later read on a failed reader stays a no-op (no over-read, sentinel return).
    REQUIRE(r.u8() == 0);
    REQUIRE_FALSE(r.ok());
}

TEST_CASE("wire::reader mid-field truncation between two fields fails closed", "[wire][cursor]")
{
    std::array<std::byte, sizeof(std::uint8_t) + 3> buf{};
    reader                                          r{buf};
    (void)r.u8();
    REQUIRE(r.ok());
    auto v = r.u32();
    REQUIRE(v == 0);
    REQUIRE_FALSE(r.ok());
}

TEST_CASE("wire::reader.varint() rejects an over-long continuation exactly like read_varint", "[wire][cursor]")
{
    std::array<std::byte, 11> overlong{};
    overlong.fill(std::byte{0x80});

    std::size_t consumed = 0;
    auto        direct   = read_varint(std::span<const std::byte>{overlong}, consumed);
    REQUIRE_FALSE(direct.has_value());

    reader r{overlong};
    auto   via_cursor = r.varint();
    REQUIRE_FALSE(via_cursor.has_value());
    REQUIRE_FALSE(r.ok());
}

TEST_CASE("wire::reader.varint() round-trips a well-formed varint and advances the cursor", "[wire][cursor]")
{
    std::vector<std::byte> buf;
    write_varint(buf, 300u);
    buf.push_back(std::byte{0xEE});

    reader r{buf};
    auto   value = r.varint();
    REQUIRE(value.has_value());
    REQUIRE(*value == 300u);
    REQUIRE(r.ok());
    REQUIRE(r.u8() == 0xEE);
    REQUIRE(r.ok());
}

TEST_CASE("wire::writer into a reserved buffer does not reallocate", "[wire][cursor][noalloc]")
{
    constexpr std::size_t  frame_size = sizeof(std::uint8_t) + 2 * sizeof(std::uint64_t);
    std::vector<std::byte> buf;
    buf.resize(frame_size);

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < 1024; ++i)
    {
        buf.resize(frame_size);
        writer w{buf};
        w.u8(0x11);
        w.u64(0x2222222222222222ull);
        w.u64(0x3333333333333333ull);
    }
    const auto after = plexus::testing::alloc_count();
    REQUIRE(after - before == 0);
}
