#include "test_crc_serial_common.h"

#include "support/serial_kat.h"
#include "support/serial_bytes.h"

#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <algorithm>

using namespace crc_serial_test;
using plexus::testing::from_hex;
using plexus::testing::serial_kat_vector;
using plexus::testing::serial_kat_vectors;

namespace {

std::vector<std::string> wire_frame_of(const serial_kat_vector &row)
{
    const auto frame = encode_frame(row.header, from_hex(row.payload_hex));
    return {std::string{reinterpret_cast<const char *>(frame.data()), frame.size()}};
}

std::vector<std::string> deframe(std::span<const std::byte> first, std::span<const std::byte> second = {})
{
    sink s;
    auto dec = s.make();
    dec.feed(first);
    dec.feed(second);
    return s.emitted;
}

}

TEST_CASE("crc32c kat: the canonical Castagnoli check vector", "[wire][crc32c][kat]")
{
    // The check value of CRC-32C, the Castagnoli polynomial iSCSI specifies (RFC 3720), over the ASCII "123456789".
    REQUIRE(crc32c(as_bytes("123456789")) == 0xE3069283u);
}

TEST_CASE("crc32c kat: the empty-input identity", "[wire][crc32c][kat]")
{
    REQUIRE(crc32c({}) == 0x00000000u);
}

TEST_CASE("serial kat: every row encodes to its pinned bytes", "[wire][serial][kat]")
{
    for(const auto &row : serial_kat_vectors)
    {
        INFO(row.name);
        const auto literal = from_hex(row.frame_hex);
        const auto payload = from_hex(row.payload_hex);
        REQUIRE(row.header.payload_len == payload.size());
        REQUIRE(literal.size() == serial_header_size + payload.size() + crc_trailer_size);
        REQUIRE(std::equal(row.format_id.begin(), row.format_id.end(), literal.begin()));
        REQUIRE(plexus::testing::serial_bytes(encode_frame(row.header, payload)) == literal);
    }
}

TEST_CASE("serial kat: every row decodes whole and split at every byte into its wire frame", "[wire][serial][kat]")
{
    for(const auto &row : serial_kat_vectors)
    {
        INFO(row.name);
        const auto literal = from_hex(row.frame_hex);
        const std::span<const std::byte> bytes{literal};
        REQUIRE(deframe(bytes) == wire_frame_of(row));
        for(std::size_t cut = 1; cut < bytes.size(); ++cut)
        {
            CAPTURE(cut);
            REQUIRE(deframe(bytes.first(cut), bytes.subspan(cut)) == wire_frame_of(row));
        }
    }
}

TEST_CASE("serial kat: a row with any one byte dropped, then intact, yields exactly its wire frame once", "[wire][serial][kat]")
{
    for(const auto &row : serial_kat_vectors)
    {
        INFO(row.name);
        const auto literal = from_hex(row.frame_hex);
        for(std::size_t at = 0; at < literal.size(); ++at)
        {
            CAPTURE(at);
            auto damaged = literal;
            damaged.erase(damaged.begin() + static_cast<std::ptrdiff_t>(at));
            damaged.insert(damaged.end(), literal.begin(), literal.end());
            REQUIRE(deframe(damaged) == wire_frame_of(row));
        }
    }
}
