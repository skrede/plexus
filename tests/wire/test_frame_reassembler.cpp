#include "test_frame_reassembler_common.h"

TEST_CASE("reassembler extracts single complete frame", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload{std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}, std::byte{0xDD}, std::byte{0xEE}};
    auto                   frame = make_frame(msg_type::unidirectional, 0, 1000, payload);

    auto result = ra.feed(frame);

    CHECK(result.error == feed_error::none);
    REQUIRE(result.frames.size() == 1);
    CHECK(result.frames[0].header.type == msg_type::unidirectional);
    CHECK(result.frames[0].header.payload_len == 5);
    CHECK(result.frames[0].payload == std::span<const std::byte>(frame));
    CHECK(ra.buffered_bytes() == 0);
}

TEST_CASE("reassembler extracts multiple frames from single feed", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> p1{std::byte{0x01}, std::byte{0x02}};
    std::vector<std::byte> p2{std::byte{0x03}, std::byte{0x04}, std::byte{0x05}};

    auto f1 = make_frame(msg_type::unidirectional, 0, 100, p1);
    auto f2 = make_frame(msg_type::bidirectional, 0, 200, p2);

    std::vector<std::byte> combined;
    combined.insert(combined.end(), f1.begin(), f1.end());
    combined.insert(combined.end(), f2.begin(), f2.end());

    auto result = ra.feed(combined);

    CHECK(result.error == feed_error::none);
    REQUIRE(result.frames.size() == 2);
    CHECK(result.frames[0].header.type == msg_type::unidirectional);
    CHECK(result.frames[0].payload == std::span<const std::byte>(f1));
    CHECK(result.frames[1].header.type == msg_type::bidirectional);
    CHECK(result.frames[1].payload == std::span<const std::byte>(f2));
}

TEST_CASE("reassembler handles header split mid-way", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload{std::byte{0xFF}};
    auto                   frame = make_frame(msg_type::unidirectional, 0, 500, payload);

    auto first_half  = std::span<const std::byte>{frame}.subspan(0, 10);
    auto second_half = std::span<const std::byte>{frame}.subspan(10);

    auto r1 = ra.feed(first_half);
    CHECK(r1.frames.empty());
    CHECK(ra.buffered_bytes() == 10);

    auto r2 = ra.feed(second_half);
    CHECK(r2.error == feed_error::none);
    REQUIRE(r2.frames.size() == 1);
    CHECK(r2.frames[0].header.type == msg_type::unidirectional);
    CHECK(r2.frames[0].payload == std::span<const std::byte>(frame));
}

TEST_CASE("reassembler handles payload split", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload(100, std::byte{0x42});
    auto                   frame = make_frame(msg_type::unidirectional, 0, 700, payload);

    auto header_plus_half = std::span<const std::byte>{frame}.subspan(0, header_size + 50);
    auto rest             = std::span<const std::byte>{frame}.subspan(header_size + 50);

    auto r1 = ra.feed(header_plus_half);
    CHECK(r1.frames.empty());

    auto r2 = ra.feed(rest);
    CHECK(r2.error == feed_error::none);
    REQUIRE(r2.frames.size() == 1);
    CHECK(r2.frames[0].payload == std::span<const std::byte>(frame));
}

TEST_CASE("reassembler handles single-byte drip feed", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload{std::byte{0x10}, std::byte{0x20}, std::byte{0x30}};
    auto                   frame = make_frame(msg_type::unidirectional, 0, 900, payload);

    REQUIRE(frame.size() == header_size + 3);

    for(std::size_t i = 0; i + 1 < frame.size(); ++i)
    {
        auto r = ra.feed(std::span<const std::byte>{&frame[i], 1});
        CHECK(r.frames.empty());
    }

    auto final_result = ra.feed(std::span<const std::byte>{&frame.back(), 1});
    CHECK(final_result.error == feed_error::none);
    REQUIRE(final_result.frames.size() == 1);
    CHECK(final_result.frames[0].payload == std::span<const std::byte>(frame));
}

TEST_CASE("reassembler returns empty on zero-length feed", "[wire][reassembler]")
{
    frame_reassembler ra;
    auto              result = ra.feed(std::span<const std::byte>{});

    CHECK(result.error == feed_error::none);
    CHECK(result.frames.empty());
}

TEST_CASE("reassembler reports invalid magic error", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload{std::byte{0x01}};
    auto                   frame = make_frame(msg_type::unidirectional, 0, 1000, payload);

    frame[0] = std::byte{0xFF};

    auto result = ra.feed(frame);
    CHECK(result.error == feed_error::invalid_magic);
    CHECK(result.frames.empty());
    CHECK(ra.buffered_bytes() == 0);
}

TEST_CASE("reassembler reports payload too large error", "[wire][reassembler]")
{
    frame_reassembler ra(100);

    frame_header hdr{.type = msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 1000, .payload_len = 200};
    auto         header_bytes = encode_header(hdr);

    auto result = ra.feed(header_bytes);
    CHECK(result.error == feed_error::payload_too_large);
    CHECK(result.frames.empty());
    CHECK(ra.buffered_bytes() == 0);
}

TEST_CASE("reassembler reset clears state", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload{std::byte{0xAA}};
    auto                   frame = make_frame(msg_type::unidirectional, 0, 1000, payload);

    auto partial = std::span<const std::byte>{frame}.subspan(0, 10);
    ra.feed(partial);
    CHECK(ra.buffered_bytes() == 10);

    ra.reset();
    CHECK(ra.buffered_bytes() == 0);

    auto result = ra.feed(frame);
    CHECK(result.error == feed_error::none);
    REQUIRE(result.frames.size() == 1);
    CHECK(result.frames[0].payload == std::span<const std::byte>(frame));
}

TEST_CASE("reassembler buffered_bytes reports correctly", "[wire][reassembler]")
{
    frame_reassembler      ra;
    std::vector<std::byte> payload{std::byte{0x01}, std::byte{0x02}};
    auto                   frame = make_frame(msg_type::unidirectional, 0, 1000, payload);

    auto partial = std::span<const std::byte>{frame}.subspan(0, 15);
    ra.feed(partial);
    CHECK(ra.buffered_bytes() == 15);

    auto rest   = std::span<const std::byte>{frame}.subspan(15);
    auto result = ra.feed(rest);
    CHECK(result.frames.size() == 1);
    CHECK(ra.buffered_bytes() == 0);
}
