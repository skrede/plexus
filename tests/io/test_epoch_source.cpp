// The epoch well that mints each incarnation's session_id. These tests pin the
// two invariants the staleness gate relies on: every minted epoch is non-zero (0
// is the unestablished sentinel reserved for handshake control frames), and the
// well mints strictly-distinct values well past 255 — the u8 width that wrapped at
// 256 (re-yielding 1) is retired, so a long-lived reused slot never re-emits a
// prior epoch.

#include "plexus/io/epoch_source.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <cstdint>

using plexus::io::epoch_source;

TEST_CASE("epoch_source: mints strictly-distinct, non-zero epochs past the u8 boundary", "[io][epoch_source]")
{
    epoch_source well;
    std::set<std::uint64_t> seen;

    // 600 mints crosses the old u8 wrap (256) more than twice over: under u8 the
    // 256th mint would have re-yielded 1, colliding with the first.
    constexpr int k_mints = 600;
    for(int i = 0; i < k_mints; ++i)
    {
        const auto epoch = well.next();
        CHECK(epoch != 0);                // 0 is the reserved unestablished sentinel
        CHECK(seen.insert(epoch).second); // strictly distinct — no wrap collision
    }

    REQUIRE(seen.size() == k_mints);
    // The 256th distinct epoch is 256, not the u8-wrapped 1.
    CHECK(seen.count(256) == 1);
    CHECK(*seen.rbegin() == k_mints);
}

TEST_CASE("epoch_source: current() tracks the last minted epoch", "[io][epoch_source]")
{
    epoch_source well;
    CHECK(well.current() == 0); // nothing minted yet
    const auto first = well.next();
    CHECK(well.current() == first);
    const auto second = well.next();
    CHECK(well.current() == second);
    CHECK(second != first);
}
