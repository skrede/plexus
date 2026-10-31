#include "test_crc_serial_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>

// The happy-path legs of the decorator oracle: a valid frame emits its clean header+payload
// bytes, cold-join garbage before the first anchor is skipped, a frame split across feeds is
// reassembled, and an empty-payload frame round-trips. The corruption/resync/bounded legs live
// in test_crc_serial_resync.cpp (the two compile into one test binary).

using namespace crc_serial_test;

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

TEST_CASE("crc_serial: garbage before the first magic anchor is skipped and the frame decodes", "[wire][crc_serial]")
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

TEST_CASE("crc_serial: a frame split across two feeds is reassembled and emitted once", "[wire][crc_serial]")
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

TEST_CASE("crc_serial: an empty-payload frame round-trips (trailer over the bare header)", "[wire][crc_serial]")
{
    sink s;
    auto dec   = s.make();
    auto frame = framed("");
    dec.feed(frame);

    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on(""));
}

TEST_CASE("crc_serial: a damaged header is refused once its 32 bytes arrive, without waiting for its declared length", "[wire][crc_serial]")
{
    sink s;
    auto dec   = s.make();
    auto frame = framed("damaged-header");
    frame[plexus::testing::k_serial_timestamp_offset] ^= std::byte{0x01};
    dec.feed(std::span<const std::byte>{frame}.first(serial_header_size));

    REQUIRE(s.drops == 1);
    REQUIRE(s.emitted.empty());
}

TEST_CASE("crc_serial: bytes under a foreign format id are never emitted", "[wire][crc_serial]")
{
    sink s;
    auto dec        = s.make();
    auto stream     = framed("foreign-id");
    stream[0]       = std::byte{0x12};
    stream[1]       = std::byte{0x34};
    const auto good = framed("native-id");
    stream.insert(stream.end(), good.begin(), good.end());
    dec.feed(stream);

    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == header_on("native-id"));
}

TEST_CASE("crc_serial: an emitted frame is the wire frame", "[wire][crc_serial]")
{
    sink s;
    auto dec           = s.make();
    const auto payload = as_bytes("wire-frame");
    frame_header hdr{.type = msg_type::bidirectional, .flags = 0x05, .session_id = 0x0102030405060708u, .timestamp_ns = 0x1112131415161718u, .payload_len = 0};
    const auto wire = encode_frame(hdr, payload);
    dec.feed(plexus::testing::serial_bytes(wire));

    REQUIRE(s.drops == 0);
    REQUIRE(s.emitted.size() == 1);
    REQUIRE(s.emitted[0] == std::string(reinterpret_cast<const char *>(wire.data()), wire.size()));
    REQUIRE(std::byte(s.emitted[0][0]) == magic_byte_0);
    REQUIRE(std::byte(s.emitted[0][1]) == magic_byte_1);
}
