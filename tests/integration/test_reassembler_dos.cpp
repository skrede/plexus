#include "test_reassembler_dos_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reassembler_dos_fixture;

TEST_CASE("integration.reassembler_dos a total-memory-cap flood is rejected and stays bounded", "[reassembler][dos][cap]")
{
    constexpr std::size_t frag       = 256;
    constexpr std::uint16_t frag_cnt = 2;
    // The cap counts payload AND the per-entry slot/present metadata, so a 2-fragment entry
    // costs its payload plus the structural overhead of two slots and a 2-bit present map.
    const std::size_t overhead  = frag_cnt * sizeof(std::vector<std::byte>) + (frag_cnt + 7u) / 8u;
    const std::size_t per_entry = frag + overhead;
    const std::size_t cap       = 4 * per_entry; // room for at most four such partials

    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler r{h.ex, {.total_memory_cap = cap}};

        bool delivered = false;
        r.on_deliver([&](std::span<const std::byte>) { delivered = true; });

        // A flood of distinct msg_ids each claiming a 2-fragment message: the first four
        // open and hold one fragment each (filling the cap); every subsequent NEW partial
        // is refused with dropped_cap. The held bytes never exceed the cap.
        std::size_t admitted = 0, capped = 0;
        for(std::uint16_t id = 1; id <= 200; ++id)
        {
            const auto out = r.feed(id, 0, frag_cnt, filler(frag));
            if(out == test_reassembler::outcome::admitted)
                ++admitted;
            else if(out == test_reassembler::outcome::dropped_cap)
                ++capped;
            REQUIRE(r.held_bytes() <= cap); // the hard bound holds under the flood
        }
        REQUIRE(admitted == 4); // exactly the cap's worth opened
        REQUIRE(capped == 196); // the rest refused, no growth
        REQUIRE(r.in_flight() == 4);
        REQUIRE_FALSE(delivered); // no message completed
    }
}

TEST_CASE("integration.reassembler_dos a metadata-amplification flood (tiny datagrams claiming a "
          "huge frag_cnt) is refused",
          "[reassembler][dos][cap][amplification]")
{
    // The amplification attack the payload-only cap let through: a ~1-byte datagram claiming
    // the maximum frag_cnt forces ~786 KB of slot/present metadata while charging one payload
    // byte. With the structural cost now charged at open_entry, a 16 MiB cap admits only a
    // bounded handful of such entries and refuses the rest — the cap is a true memory bound,
    // not a payload-only bound. Without the fix this loop would mint thousands of entries and
    // exhaust gigabytes while held_bytes reported only a few kilobytes.
    constexpr std::uint16_t huge_cnt = plexus::io::max_fragment_count; // 32768
    const std::size_t structural     = huge_cnt * sizeof(std::vector<std::byte>) + (huge_cnt + 7u) / 8u;
    constexpr std::size_t cap        = 16u * 1024u * 1024u;
    const std::size_t expected_admit = cap / (structural + 1u); // +1 for the single payload byte

    for(int loop = 0; loop < 4; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler r{h.ex, {.total_memory_cap = cap}};

        std::size_t admitted = 0, capped = 0;
        for(std::uint16_t id = 1; id <= 2000; ++id)
        {
            const auto out = r.feed(id, 0, huge_cnt, filler(1));
            if(out == test_reassembler::outcome::admitted)
                ++admitted;
            else if(out == test_reassembler::outcome::dropped_cap)
                ++capped;
            REQUIRE(r.held_bytes() <= cap); // structural cost counts: never past the cap
        }
        REQUIRE(admitted == expected_admit); // only the cap's worth of slot tables opened
        REQUIRE(admitted < 25);              // a true bound — not thousands of entries
        REQUIRE(capped == 2000 - admitted);  // every further amplification attempt refused
        REQUIRE(r.in_flight() == admitted);
    }
}

TEST_CASE("integration.reassembler_dos a per-message-ceiling overrun is dropped", "[reassembler][dos][ceiling]")
{
    constexpr std::size_t ceiling = 1024;

    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler r{h.ex, {.max_message_size = ceiling, .total_memory_cap = 64u * 1024u}};

        // Two fragments that fit, then a third whose bytes would push the entry past the
        // per-message ceiling: the third is dropped_malformed, the entry stays bounded.
        // held_bytes counts the two stored fragments plus the one entry's slot metadata.
        const std::size_t overhead = 4 * sizeof(std::vector<std::byte>) + (4u + 7u) / 8u;
        REQUIRE(r.feed(1, 0, 4, filler(512)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(1, 1, 4, filler(512)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(1, 2, 4, filler(512)) == test_reassembler::outcome::dropped_malformed);
        REQUIRE(r.held_bytes() == 1024 + overhead); // the over-ceiling fragment added nothing
        REQUIRE(r.in_flight() == 1);
    }
}
