#include "test_crc_serial_common.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <string>
#include <cstddef>

// The corruption/resync/boundedness legs of the decorator oracle: a flipped trailer or header
// byte is caught by the CRC and the link resyncs onto the next frame, and a lying oversized
// payload_len stays bounded (no over-read, no over-alloc) and resyncs — all without ever
// closing or aborting.

using namespace crc_serial_test;

TEST_CASE(
        "crc_serial: a flipped trailer byte drops the frame and resyncs onto the next valid frame",
        "[wire][crc_serial]")
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

TEST_CASE("crc_serial: a flipped HEADER byte is caught by the CRC and resyncs onto the next frame",
          "[wire][crc_serial]")
{
    sink s;
    auto dec = s.make();

    auto bad = framed("hdr-corrupt");
    // Flip a header byte that does NOT move payload_len (the flags byte at offset 3): the
    // frame length and trailer position are unchanged, but the CRC over [header||payload]
    // no longer matches the trailer -> the corruption is caught and the frame is dropped.
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

TEST_CASE("crc_serial: a lying oversized payload_len stays bounded — no over-read, no over-alloc",
          "[wire][crc_serial]")
{
    // A tiny payload cap so a crafted header that claims more than the cap exercises the
    // lying-length branch deterministically (independent of the 16 MiB default).
    crc_serial_inbound dec{/*max_payload=*/16};
    int                drops = 0;
    std::string        out;
    dec.on_match([&](std::span<const std::byte> f)
                 { out.assign(reinterpret_cast<const char *>(f.data()), f.size()); });
    dec.on_drop(
            [&](close_cause c)
            {
                if(c == close_cause::crc_mismatch)
                    ++drops;
            });

    // A well-formed header whose declared payload_len (4096) dwarfs the 16-byte cap, with
    // only a few trailing bytes: the decorator must NOT wait to buffer 4096 bytes nor read
    // past the buffer — it drops on the over-cap length and resyncs.
    auto                   hdr = encode_header(make_header(4096));
    std::vector<std::byte> stream{hdr.begin(), hdr.end()};
    for(int i = 0; i < 8; ++i)
        stream.push_back(std::byte{0x00});
    const auto good = framed("ok"); // 2-byte payload <= 16-byte cap
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(drops >= 1);
    REQUIRE(out == std::string(reinterpret_cast<const char *>(good.data()), header_size + 2));
}
