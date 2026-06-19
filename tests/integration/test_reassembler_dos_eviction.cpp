#include "test_reassembler_dos_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reassembler_dos_fixture;

TEST_CASE("integration.reassembler_dos malformed fragments are rejected without indexing past the "
          "span",
          "[reassembler][dos][malformed]")
{
    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler         r{h.ex};

        // idx >= cnt, cnt == 0, and cnt past the max_fragment_count ceiling are each rejected
        // before any indexing — looped so a state-carrying regression is caught.
        REQUIRE(r.feed(1, 5, 3, filler(64)) ==
                test_reassembler::outcome::dropped_malformed); // idx >= cnt
        REQUIRE(r.feed(1, 0, 0, filler(64)) ==
                test_reassembler::outcome::dropped_malformed); // cnt == 0
        REQUIRE(r.feed(1, 0, 0xFFFF, filler(64)) ==
                test_reassembler::outcome::dropped_malformed); // cnt over max
        REQUIRE(r.in_flight() == 0);
        REQUIRE(r.held_bytes() == 0);

        // Now that frag_cnt is a uint32 wire field, a forged fragment can claim a count near
        // 2^32. The malformed gate (frag_cnt > max_fragment_count) refuses it BEFORE open_entry
        // runs, so neither structural_cost is charged nor any slot table allocated — the widened
        // field cannot be turned into a metadata-amplification allocation. structural_cost itself
        // casts to size_t before the multiply, so even the rejected count cannot overflow the
        // cost computation on the path that does evaluate it.
        REQUIRE(r.feed(1, 0, 0xFFFFFFFFu, filler(64)) ==
                test_reassembler::outcome::dropped_malformed);
        REQUIRE(r.feed(1, 0, 0xFFFFFFF0u, filler(64)) ==
                test_reassembler::outcome::dropped_malformed);
        REQUIRE(r.in_flight() == 0);
        REQUIRE(r.held_bytes() == 0);

        // A frag_cnt that disagrees with an already-open entry (an oversize idx for the
        // opened count) is dropped_malformed too — the per-entry slot bound holds.
        REQUIRE(r.feed(2, 0, 2, filler(64)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(2, 9, 2, filler(64)) == test_reassembler::outcome::dropped_malformed);
        REQUIRE(r.in_flight() == 1);
    }
}

TEST_CASE("integration.reassembler_dos a stalled partial is evicted on the per-message timeout",
          "[reassembler][dos][timeout]")
{
    constexpr auto timeout = 1000ms;

    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler         r{h.ex, {.per_message_timeout = timeout}};

        bool delivered = false;
        r.on_deliver([&](std::span<const std::byte>) { delivered = true; });

        // A flood of stalled partials: each opens with one fragment of a multi-fragment
        // message that never completes. Without the timer path these would accumulate;
        // the per-message timeout reclaims every one.
        const std::size_t overhead = 4 * sizeof(std::vector<std::byte>) + (4u + 7u) / 8u;
        for(std::uint16_t id = 1; id <= 32; ++id)
            REQUIRE(r.feed(id, 0, 4, filler(128)) == test_reassembler::outcome::admitted);
        REQUIRE(r.in_flight() == 32);
        REQUIRE(r.held_bytes() == 32u * (128u + overhead)); // payload + per-entry slot metadata

        // Cross the timeout: every stalled partial is evicted, the state returns to zero,
        // and nothing was delivered (best-effort drop-whole: a lost-fragment message is gone).
        h.advance(std::chrono::duration_cast<plexus::testing::test_clock::duration>(1500ms));

        REQUIRE_FALSE(delivered);
        REQUIRE(r.in_flight() == 0);
        REQUIRE(r.held_bytes() == 0);

        // After eviction the reassembler still admits fresh work (no wedged state).
        REQUIRE(r.feed(99, 0, 1, filler(64)) == test_reassembler::outcome::completed);
        REQUIRE(delivered);
    }
}

TEST_CASE("integration.reassembler_dos the demux cap bounds the spoofed-source channel count",
          "[reassembler][dos][demux]")
{
    // Each datagram channel is per-peer and owns one reassembler, so the demux peer cap
    // bounds how many reassemblers a spoofed-source flood can mint — the aggregate
    // reassembly-memory bound is (demux cap × per-reassembler cap), not unbounded.
    using demux =
            plexus::io::detail::basic_inbound_demux<int, std::uint32_t, std::hash<std::uint32_t>>;

    constexpr std::size_t cap = 8;
    demux                 d{cap};
    int                   channels[cap];

    // A spoofed-source flood of distinct endpoints fills exactly the cap, then every
    // further NEW source is refused (insert returns false) so the caller drops it rather
    // than minting an unbounded channel/reassembler set.
    for(std::uint32_t ep = 0; ep < cap; ++ep)
        REQUIRE(d.insert(ep, &channels[ep]));
    REQUIRE(d.size() == cap);

    std::size_t refused = 0;
    for(std::uint32_t ep = 100; ep < 100 + 50; ++ep)
        if(!d.insert(ep, &channels[0]))
            ++refused;
    REQUIRE(refused == 50);   // every new source past the cap is refused
    REQUIRE(d.size() == cap); // the live peer count never grows past the cap

    // An OVERWRITE of an already-mapped source does not grow the map (a re-dial reuses the
    // slot), so a same-source re-accept cannot bypass the cap.
    REQUIRE(d.insert(0, &channels[1]));
    REQUIRE(d.size() == cap);

    // The default cap is the production aggregate bound: a 4096-peer ceiling.
    REQUIRE(demux::default_max_peers == 4096);
}
