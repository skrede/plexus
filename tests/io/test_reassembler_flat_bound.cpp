// The reassembler structural-cost bound oracle: the total-memory cap charges the
// per-entry slot/present metadata a claimed frag_cnt forces, NOT just the held payload.
// A tiny datagram claiming a huge frag_cnt cannot mint that metadata accounted as one
// byte — the open is refused with dropped_cap and held_bytes() never exceeds the cap.
// A flood of distinct attacker msg_ids past the cap-implied max partials cannot grow the
// in-flight table unbounded. These bounds hold regardless of the entry container, so the
// oracle pins behavior any later container swap must preserve verbatim.

#include "plexus/io/detail/reassembler.h"

#include "plexus/testing/harness.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus;

namespace {

using test_reassembler =
        io::detail::reassembler<plexus::inproc::inproc_executor<testing::test_clock> &,
                                plexus::inproc::inproc_timer<testing::test_clock>>;

std::vector<std::byte> one_byte() { return std::vector<std::byte>(1, std::byte{0xAB}); }

}

TEST_CASE("reassembler refuses a tiny datagram claiming a huge frag_cnt at the structural cost",
          "[reassemble][bound][dos]")
{
    testing::harness h;
    // A cap far below the slot-table cost a frag_cnt=32768 open would charge: structural_cost
    // alone is hundreds of KiB, so a 64 KiB cap cannot admit the entry.
    constexpr std::uint16_t frag_cnt = 32768;
    constexpr std::size_t   cap      = 64u * 1024u;
    REQUIRE(test_reassembler::structural_cost(frag_cnt) > cap);

    test_reassembler r{h.ex, {.total_memory_cap = cap}};

    // The crafted open is refused at the cap (the structural charge alone breaches it) —
    // not silently accounted as a single payload byte.
    CHECK(r.feed(1, 0, frag_cnt, one_byte()) == test_reassembler::outcome::dropped_cap);
    CHECK(r.in_flight() == 0);
    CHECK(r.held_bytes() == 0);
    CHECK(r.held_bytes() <= cap);
}

TEST_CASE("reassembler keeps held_bytes within the cap as crafted opens are refused",
          "[reassemble][bound][dos]")
{
    testing::harness      h;
    constexpr std::size_t cap = 256u * 1024u;
    test_reassembler      r{h.ex, {.total_memory_cap = cap}};

    // Each distinct msg_id claims a frag_cnt whose structural cost is a meaningful slice of
    // the cap; once the table fills the cap, every further distinct id is refused. The held
    // byte count never crosses the cap regardless of how many ids the attacker tries.
    constexpr std::uint16_t frag_cnt = 4096;
    for(std::uint16_t id = 1; id <= 5000; ++id)
    {
        const auto o = r.feed(id, 0, frag_cnt, one_byte());
        CHECK((o == test_reassembler::outcome::admitted ||
               o == test_reassembler::outcome::dropped_cap));
        CHECK(r.held_bytes() <= cap);
    }

    // The in-flight table is bounded by the cap divided by the per-entry structural cost —
    // a flood of distinct ids cannot grow it without bound.
    const std::size_t per_entry = test_reassembler::structural_cost(frag_cnt);
    CHECK(r.in_flight() <= cap / per_entry + 1);
    CHECK(r.held_bytes() <= cap);
}
