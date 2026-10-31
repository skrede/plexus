#include "test_crc_serial_common.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <limits>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace crc_serial_test;

namespace {

constexpr std::size_t k_cap = 16;

std::vector<std::byte> intact_header(std::size_t payload_len)
{
    const auto hdr = encode_serial_header(encode_header(make_header(payload_len)));
    return {hdr.begin(), hdr.end()};
}

void append(std::vector<std::byte> &out, const std::vector<std::byte> &bytes)
{
    out.insert(out.end(), bytes.begin(), bytes.end());
}

std::vector<std::byte> anchored_garbage(std::size_t size)
{
    std::vector<std::byte> out(size, std::byte{0xA5});
    for(std::size_t i = 0; i + 1 < size; i += 5)
    {
        out[i]     = k_serial_format_id[0];
        out[i + 1] = k_serial_format_id[1];
    }
    return out;
}

}

TEST_CASE("crc_serial: an intact header over the cap is refused as payload_too_large and the next frame decodes", "[wire][crc_serial]")
{
    sink s;
    auto dec = s.make(k_cap);

    auto stream = intact_header(k_cap + 1);
    append(stream, framed("next"));
    dec.feed(stream);

    REQUIRE(s.over_cap == 1);
    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on("next"));
}

TEST_CASE("crc_serial: a damaged header or trailer is still refused as crc_mismatch", "[wire][crc_serial]")
{
    sink s;
    auto dec = s.make(k_cap);

    auto bad_header = framed("hdr");
    bad_header[header_size - sizeof(std::uint64_t)] ^= std::byte{0x80};
    auto bad_trailer = framed("tlr");
    bad_trailer.back() ^= std::byte{0xFF};

    dec.feed(bad_header);
    REQUIRE(s.drops == 1);
    dec.feed(bad_trailer);
    REQUIRE(s.drops == 2);
    REQUIRE(s.over_cap == 0);
    REQUIRE(s.emitted.empty());

    dec.feed(framed("ok"));
    REQUIRE(s.emitted.size() == 1);
}

TEST_CASE("crc_serial: a cap past the stream ceiling is clamped, so the capacity cannot wrap and feed returns", "[wire][crc_serial]")
{
    constexpr std::size_t size_max = std::numeric_limits<std::size_t>::max();
    for(const std::size_t cap : {size_max, size_max - (serial_header_size + crc_trailer_size - 1)})
    {
        sink s;
        auto dec = s.make(cap);
        REQUIRE(dec.max_payload() == k_max_reassembler_payload_bytes);

        auto stream = anchored_garbage(3 * serial_header_size);
        append(stream, framed("ok"));
        dec.feed(stream);

        REQUIRE(s.drops > 0);
        REQUIRE(s.emitted.size() == 1);
        REQUIRE(s.emitted[0] == header_on("ok"));
    }
}
