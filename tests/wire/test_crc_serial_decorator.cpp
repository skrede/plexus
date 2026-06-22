#include "plexus/wire/crc_serial.h"
#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

// Pure unit oracle for the serial CRC32C trailer + magic-resync decorator. No
// transport: byte buffers are fed directly and the emitted clean frames + the drop
// signals are asserted. The decorator must verify a trailer, drop+resync a corrupt
// frame on the magic anchor WITHOUT ever closing/aborting, and stay bounded on a
// lying header length.

using namespace plexus::wire;

namespace {

std::span<const std::byte> as_bytes(std::string_view s) noexcept
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

frame_header make_header(std::size_t payload_len)
{
    frame_header hdr{};
    hdr.type        = msg_type::unidirectional;
    hdr.flags       = 0;
    hdr.session_id  = 1;
    hdr.payload_len = payload_len;
    return hdr;
}

// Build a complete serial frame: [header][payload][CRC32C trailer LE].
std::vector<std::byte> framed(std::string_view payload)
{
    const auto pl     = as_bytes(payload);
    const auto hdr    = encode_header(make_header(pl.size()));
    const auto trail  = crc_trailer(std::span<const std::byte>{hdr}, pl);
    std::vector<std::byte> out;
    out.insert(out.end(), hdr.begin(), hdr.end());
    out.insert(out.end(), pl.begin(), pl.end());
    out.insert(out.end(), trail.begin(), trail.end());
    return out;
}

struct sink
{
    std::vector<std::string> emitted;
    int                      drops{0};

    crc_serial_inbound make()
    {
        crc_serial_inbound dec;
        dec.on_match([this](std::span<const std::byte> f)
                     { emitted.emplace_back(reinterpret_cast<const char *>(f.data()), f.size()); });
        dec.on_drop([this](close_cause c)
                    { if(c == close_cause::crc_mismatch) ++drops; });
        return dec;
    }
};

std::string header_on(std::string_view payload)
{
    const auto f = framed(payload);
    return std::string{reinterpret_cast<const char *>(f.data()), header_size + payload.size()};
}

}

TEST_CASE("crc_serial: a valid frame emits clean header+payload bytes, no drop", "[wire][crc_serial]")
{
    sink s;
    auto dec   = s.make();
    auto frame = framed("hello-serial");
    dec.feed(frame);

    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on("hello-serial"));
}

TEST_CASE("crc_serial: a flipped trailer byte drops the frame and resyncs onto the next valid frame",
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

    REQUIRE(s.drops == 1);                 // the corrupt frame was observed non-fatally
    REQUIRE(s.emitted.size() == 1);        // exactly the recovered frame
    REQUIRE(s.emitted[0] == header_on("recovered")); // resync recovered the boundary
}

TEST_CASE("crc_serial: garbage before the first magic anchor is skipped and the frame decodes",
          "[wire][crc_serial]")
{
    sink s;
    auto dec = s.make();

    std::vector<std::byte> stream;
    for(int i = 0; i < 7; ++i)
        stream.push_back(std::byte{0xAA}); // cold-join junk, no magic
    const auto good = framed("aligned");
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on("aligned"));
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
    sink s;
    // A tiny payload cap so a crafted header that claims more than the cap exercises the
    // lying-length branch deterministically (independent of the 16 MiB default).
    crc_serial_inbound dec{/*max_payload=*/16};
    int                drops = 0;
    std::string        out;
    dec.on_match([&](std::span<const std::byte> f)
                 { out.assign(reinterpret_cast<const char *>(f.data()), f.size()); });
    dec.on_drop([&](close_cause c) { if(c == close_cause::crc_mismatch) ++drops; });

    // A well-formed header whose declared payload_len (4096) dwarfs the 16-byte cap, with
    // only a few trailing bytes: the decorator must NOT wait to buffer 4096 bytes nor read
    // past the buffer — it drops on the over-cap length and resyncs.
    auto hdr = encode_header(make_header(4096));
    std::vector<std::byte> stream{hdr.begin(), hdr.end()};
    for(int i = 0; i < 8; ++i)
        stream.push_back(std::byte{0x00});
    // Append a valid (within-cap) frame after the lie; the resync must reach it.
    const auto good = framed("ok"); // 2-byte payload <= 16-byte cap
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(drops >= 1);
    REQUIRE(out == std::string(reinterpret_cast<const char *>(good.data()), header_size + 2));
}

TEST_CASE("crc_serial: a frame split across two feeds is reassembled and emitted once",
          "[wire][crc_serial]")
{
    sink s;
    auto dec   = s.make();
    auto frame = framed("split-across-feeds");

    const std::size_t cut = frame.size() / 2;
    dec.feed(std::span<const std::byte>{frame}.subspan(0, cut));
    REQUIRE(s.emitted.empty()); // not yet complete
    dec.feed(std::span<const std::byte>{frame}.subspan(cut));

    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on("split-across-feeds"));
}

TEST_CASE("crc_serial: an empty-payload frame round-trips (trailer over the bare header)",
          "[wire][crc_serial]")
{
    sink s;
    auto dec   = s.make();
    auto frame = framed("");
    dec.feed(frame);

    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on(""));
}
