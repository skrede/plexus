// The reassembly-DoS bound: the reassembler stays bounded under a forged/partial-fragment
// flood. Each of its four bounds is FORCED, looped, and the in-flight state is
// asserted bounded throughout: the total-memory cap (a new partial that would breach the
// byte cap is rejected), the per-message ceiling (an oversize-claim fragment past
// max_message_size is dropped), malformed rejection (idx>=cnt / cnt==0 / cnt over the
// field-width max — never indexing past the span), AND the per-message timeout eviction
// (the timer path reclaims a stalled partial whose fragments never complete — the path
// most likely to be missed). The cap + timeout defaults are substantiated in reassembler.h
// and exercised here at a compressed config so the test runs fast and deterministically.
//
// Under per-fragment AEAD a forged fragment dies at the tag check in
// datagram_authenticated_channel BEFORE feed runs (proven in test_aead_fragment.cpp), so the
// reassembler rarely sees forged input on the secured path; these bounds are the reassembler's
// OWN defense-in-depth, exercised directly. The demux peer cap is also checked: it bounds the
// spoofed-source channel count, so the per-channel-per-peer reassembler structure bounds the
// aggregate reassembly memory.

#include "plexus/io/detail/reassembler.h"
#include "plexus/io/detail/inbound_demux.h"
#include "plexus/io/fragmentation.h"

#include "plexus/testing/harness.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <chrono>

using namespace std::chrono_literals;

namespace {

using test_reassembler =
        plexus::io::detail::reassembler<plexus::inproc::inproc_executor<plexus::testing::test_clock> &,
                                        plexus::inproc::inproc_timer<plexus::testing::test_clock>>;

std::vector<std::byte> filler(std::size_t n)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((i * 17u + 3u) & 0xFFu);
    return v;
}

}

TEST_CASE("integration.reassembler_dos a total-memory-cap flood is rejected and stays bounded", "[reassembler][dos][cap]")
{
    constexpr std::size_t frag = 256;
    constexpr std::size_t cap = 4 * frag;   // room for at most four such fragments

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
            const auto out = r.feed(id, 0, 2, filler(frag));
            if(out == test_reassembler::outcome::admitted)
                ++admitted;
            else if(out == test_reassembler::outcome::dropped_cap)
                ++capped;
            REQUIRE(r.held_bytes() <= cap);          // the hard bound holds under the flood
        }
        REQUIRE(admitted == 4);                       // exactly the cap's worth opened
        REQUIRE(capped == 196);                       // the rest refused, no growth
        REQUIRE(r.in_flight() == 4);
        REQUIRE_FALSE(delivered);                     // no message completed
    }
}

TEST_CASE("integration.reassembler_dos a per-message-ceiling overrun is dropped", "[reassembler][dos][ceiling]")
{
    constexpr std::size_t ceiling = 1024;

    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler r{h.ex, {.max_message_size = ceiling,
                                  .total_memory_cap = 64u * 1024u}};

        // Two fragments that fit, then a third whose bytes would push the entry past the
        // per-message ceiling: the third is dropped_malformed, the entry stays bounded.
        REQUIRE(r.feed(1, 0, 4, filler(512)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(1, 1, 4, filler(512)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(1, 2, 4, filler(512)) == test_reassembler::outcome::dropped_malformed);
        REQUIRE(r.held_bytes() == 1024);              // the over-ceiling fragment added nothing
        REQUIRE(r.in_flight() == 1);
    }
}

TEST_CASE("integration.reassembler_dos malformed fragments are rejected without indexing past the span", "[reassembler][dos][malformed]")
{
    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler r{h.ex};

        // idx >= cnt, cnt == 0, and cnt past the uint16 field-width max are each rejected
        // before any indexing — looped so a state-carrying regression is caught.
        REQUIRE(r.feed(1, 5, 3, filler(64)) == test_reassembler::outcome::dropped_malformed);   // idx >= cnt
        REQUIRE(r.feed(1, 0, 0, filler(64)) == test_reassembler::outcome::dropped_malformed);   // cnt == 0
        REQUIRE(r.feed(1, 0, 0xFFFF, filler(64)) == test_reassembler::outcome::dropped_malformed); // cnt over max
        REQUIRE(r.in_flight() == 0);
        REQUIRE(r.held_bytes() == 0);

        // A frag_cnt that disagrees with an already-open entry (an oversize idx for the
        // opened count) is dropped_malformed too — the per-entry slot bound holds.
        REQUIRE(r.feed(2, 0, 2, filler(64)) == test_reassembler::outcome::admitted);
        REQUIRE(r.feed(2, 9, 2, filler(64)) == test_reassembler::outcome::dropped_malformed);
        REQUIRE(r.in_flight() == 1);
    }
}

TEST_CASE("integration.reassembler_dos a stalled partial is evicted on the per-message timeout", "[reassembler][dos][timeout]")
{
    constexpr auto timeout = 1000ms;

    for(int loop = 0; loop < 8; ++loop)
    {
        plexus::testing::harness h;
        test_reassembler r{h.ex, {.per_message_timeout = timeout}};

        bool delivered = false;
        r.on_deliver([&](std::span<const std::byte>) { delivered = true; });

        // A flood of stalled partials: each opens with one fragment of a multi-fragment
        // message that never completes. Without the timer path these would accumulate;
        // the per-message timeout reclaims every one.
        for(std::uint16_t id = 1; id <= 32; ++id)
            REQUIRE(r.feed(id, 0, 4, filler(128)) == test_reassembler::outcome::admitted);
        REQUIRE(r.in_flight() == 32);
        REQUIRE(r.held_bytes() == 32u * 128u);

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

TEST_CASE("integration.reassembler_dos the demux cap bounds the spoofed-source channel count", "[reassembler][dos][demux]")
{
    // Each datagram channel is per-peer and owns one reassembler, so the demux peer cap
    // bounds how many reassemblers a spoofed-source flood can mint — the aggregate
    // reassembly-memory bound is (demux cap × per-reassembler cap), not unbounded.
    using demux = plexus::io::detail::basic_inbound_demux<int, std::uint32_t, std::hash<std::uint32_t>>;

    constexpr std::size_t cap = 8;
    demux d{cap};
    int channels[cap];

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
    REQUIRE(refused == 50);                 // every new source past the cap is refused
    REQUIRE(d.size() == cap);               // the live peer count never grows past the cap

    // An OVERWRITE of an already-mapped source does not grow the map (a re-dial reuses the
    // slot), so a same-source re-accept cannot bypass the cap.
    REQUIRE(d.insert(0, &channels[1]));
    REQUIRE(d.size() == cap);

    // The default cap is the production aggregate bound: a 4096-peer ceiling.
    REQUIRE(demux::default_max_peers == 4096);
}
