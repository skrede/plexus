#include "test_frame_reassembler_common.h"

TEST_CASE("reassembler reports buffer_overflow when buffered bytes exceed cap", "[wire][reassembler]")
{
    // Cap below header_size so the bytes-cap branch fires before any decode_header
    // attempt. Mirrors the trickle-attacker shape: bytes accumulate without ever
    // producing a complete header, and the cap is the only ceiling.
    constexpr std::size_t small_cap = 16;
    frame_reassembler ra(1024, small_cap);
    std::vector<std::byte> input(small_cap + 1, std::byte{0x00});

    auto result = ra.feed(input);
    CHECK(result.error == feed_error::buffer_overflow);
    CHECK(result.frames.empty());
    CHECK(ra.buffered_bytes() == 0);
}

TEST_CASE("reassembler accepts buffered bytes exactly at cap without error", "[wire][reassembler]")
{
    // Cap kept below header_size to keep this case in the pre-decode regime;
    // exact-fill must NOT trip feed_error::buffer_overflow (the trigger is
    // strict greater-than, not greater-or-equal).
    constexpr std::size_t small_cap = 16;
    frame_reassembler ra(1024, small_cap);
    std::vector<std::byte> input(small_cap, std::byte{0x00});

    auto result = ra.feed(input);
    CHECK(result.error == feed_error::none);
    CHECK_FALSE(result.error == feed_error::buffer_overflow);
    CHECK(result.frames.empty());
    CHECK(ra.buffered_bytes() == small_cap);
}

TEST_CASE("reassembler reports buffer_overflow across feed calls when accumulated bytes exceed cap", "[wire][reassembler]")
{
    // Two-step trickle: cap=20, feed 15 then 6 (total 21 > 20). Both feeds stay
    // below header_size individually so no decode_header runs until the cap fires.
    constexpr std::size_t small_cap = 20;
    frame_reassembler ra(1024, small_cap);
    std::vector<std::byte> first(15, std::byte{0x00});
    std::vector<std::byte> second(6, std::byte{0x00});

    auto r1 = ra.feed(first);
    CHECK(r1.error == feed_error::none);
    CHECK(r1.frames.empty());
    CHECK(ra.buffered_bytes() == 15);

    auto r2 = ra.feed(second);
    CHECK(r2.error == feed_error::buffer_overflow);
    CHECK(r2.frames.empty());
    CHECK(ra.buffered_bytes() == 0);
}
