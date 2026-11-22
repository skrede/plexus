#include "test_crc_serial_common.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace crc_serial_test;

namespace {

// A payload whose trailer CRC ends in the format id's first byte, so the next frame's anchor completes it when that
// last CRC byte is lost.
std::string payload_with_anchor_ended_crc()
{
    for(std::int32_t i = 0; i < 4096; ++i)
    {
        const std::string payload = "trailer-" + std::to_string(i);
        const auto header         = encode_serial_header(encode_header(make_header(payload.size())));
        if(crc_trailer(header, as_bytes(payload))[crc_trailer_size - 1] == k_serial_format_id[0])
            return payload;
    }
    return {};
}

}

TEST_CASE("crc_serial: a frame missing its last CRC byte is dropped and the next frame is delivered, even when the next frame's anchor completes the CRC", "[wire][crc_serial]")
{
    const auto payload = payload_with_anchor_ended_crc();
    REQUIRE_FALSE(payload.empty());
    sink s;
    auto dec = s.make();

    auto stream = framed(payload);
    stream.erase(stream.begin() + static_cast<std::ptrdiff_t>(serial_header_size + payload.size() + crc_trailer_size - 1));
    const auto next = framed("recovered");
    stream.insert(stream.end(), next.begin(), next.end());
    dec.feed(stream);

    REQUIRE(s.emitted == std::vector<std::string>{header_on("recovered")});
    REQUIRE(s.drops >= 1);
}

TEST_CASE("crc_serial: a frame missing its final byte is dropped and the next frame is delivered", "[wire][crc_serial]")
{
    const auto payload = payload_with_anchor_ended_crc();
    REQUIRE_FALSE(payload.empty());
    sink s;
    auto dec = s.make();

    auto stream = framed(payload);
    stream.pop_back();
    const auto next = framed("recovered");
    stream.insert(stream.end(), next.begin(), next.end());
    dec.feed(stream);

    REQUIRE(s.emitted == std::vector<std::string>{header_on("recovered")});
    REQUIRE(s.drops >= 1);
}

TEST_CASE("crc_serial: a flipped trailer byte drops the frame and resyncs onto the next valid frame", "[wire][crc_serial]")
{
    sink s;
    auto dec = s.make();

    auto bad = framed("corrupt-me");
    bad.back() ^= std::byte{0xFF}; // flip the top trailer byte -> CRC fails

    const auto good = framed("recovered");

    std::vector<std::byte> stream;
    stream.insert(stream.end(), bad.begin(), bad.end());
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(s.drops == 1);                           // the corrupt frame was observed non-fatally
    REQUIRE(s.emitted.size() == 1);                  // exactly the recovered frame
    REQUIRE(s.emitted[0] == header_on("recovered")); // resync recovered the boundary
}

TEST_CASE("crc_serial: a flipped HEADER byte is caught by the CRC and resyncs onto the next frame", "[wire][crc_serial]")
{
    sink s;
    auto dec = s.make();

    auto bad = framed("hdr-corrupt");
    // Flip a header byte that does NOT move payload_len (the flags byte at offset 3): the
    // header CRC no longer matches, so the frame is dropped once its header is buffered.
    bad[3] ^= std::byte{0x01};

    const auto good = framed("after");

    std::vector<std::byte> stream;
    stream.insert(stream.end(), bad.begin(), bad.end());
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(s.drops >= 1);          // the corruption was caught
    REQUIRE(s.emitted.size() == 1); // the following valid frame still decoded
    REQUIRE(s.emitted.back() == header_on("after"));
}

TEST_CASE("crc_serial: a lying oversized payload_len stays bounded - no over-read, no over-alloc", "[wire][crc_serial]")
{
    // A tiny payload cap so a crafted header that claims more than the cap exercises the
    // lying-length branch deterministically.
    crc_serial_inbound dec{/*max_payload=*/16};
    std::int32_t over_cap = 0;
    std::string out;
    dec.on_match([&](std::span<const std::byte> f) { out.assign(reinterpret_cast<const char *>(f.data()), f.size()); });
    dec.on_drop(
            [&](close_cause c)
            {
                if(c == close_cause::payload_too_large)
                    ++over_cap;
            });

    // A well-formed header whose declared payload_len (4096) dwarfs the 16-byte cap, with
    // only a few trailing bytes: the decorator must NOT wait to buffer 4096 bytes nor read
    // past the buffer — it drops on the over-cap length and resyncs.
    auto hdr = encode_serial_header(encode_header(make_header(4096)));
    std::vector<std::byte> stream{hdr.begin(), hdr.end()};
    for(std::int32_t i = 0; i < 8; ++i)
        stream.push_back(std::byte{0x00});
    const auto good = framed("ok"); // 2-byte payload <= 16-byte cap
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(over_cap == 1);
    REQUIRE(out == header_on("ok"));
}

TEST_CASE("crc_serial: a payload of exactly the cap is delivered and one byte more is refused at the header", "[wire][crc_serial]")
{
    constexpr std::size_t cap = 64;
    std::int32_t drops        = 0;
    std::vector<std::string> emitted;
    crc_serial_inbound dec{cap};
    dec.on_match([&](std::span<const std::byte> f) { emitted.emplace_back(reinterpret_cast<const char *>(f.data()), f.size()); });
    dec.on_drop([&](close_cause) { ++drops; });

    const std::string at_cap(cap, 'a');
    dec.feed(framed(at_cap));
    REQUIRE(drops == 0);
    REQUIRE(emitted.size() == 1);
    REQUIRE(emitted[0] == header_on(at_cap));

    const auto over = framed(std::string(cap + 1, 'b'));
    const std::span<const std::byte> bytes{over};
    dec.feed(bytes.first(serial_header_size - 1));
    REQUIRE(drops == 0);
    dec.feed(bytes.subspan(serial_header_size - 1, 1));
    REQUIRE(drops == 1);

    dec.feed(bytes.subspan(serial_header_size));
    dec.feed(framed("next"));
    REQUIRE(drops == 1);
    REQUIRE(emitted.size() == 2);
    REQUIRE(emitted[1] == header_on("next"));
}
