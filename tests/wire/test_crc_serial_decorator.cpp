#include "test_crc_serial_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
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
