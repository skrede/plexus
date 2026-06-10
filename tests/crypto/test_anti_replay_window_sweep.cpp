// The recorded sweep substantiating k_anti_replay_window_bits. It models a UDP link
// that reorders datagrams within a bounded displacement and runs each candidate
// window width over the SAME deterministic (fixed-seed, RNG-free) arrival schedule,
// counting false rejects: a legitimate never-before-seen datagram wrongly dropped as
// too-old because its sequence fell below the window floor by arrival time.
//
// Methodology mirrors the cookie_secret truncation sweep: exercise a grid of
// candidate widths, record the per-candidate failure metric, and pick the smallest
// width whose false-reject rate is zero with margin under the modeled worst case. A
// regression that shrinks the chosen width below its no-false-reject property fails
// the asserted check at the bottom. The sweep is deterministic, so two back-to-back
// runs produce identical results.

#include "plexus/crypto/anti_replay_window.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>

using plexus::crypto::anti_replay_window;
using plexus::crypto::replay_verdict;

namespace {

// A deterministic LCG (Numerical Recipes constants) — no std::random, so the schedule
// is byte-identical across runs and platforms.
class lcg
{
public:
    explicit lcg(std::uint64_t seed) noexcept : m_state(seed) {}
    std::uint64_t next() noexcept
    {
        m_state = m_state * 6364136223846793005ull + 1442695040888963407ull;
        return m_state >> 17u;
    }
    std::size_t bounded(std::size_t n) noexcept { return static_cast<std::size_t>(next() % n); }

private:
    std::uint64_t m_state;
};

// Build a reordered arrival schedule with bounded displacement: each in-order sequence
// i gets an arrival key i + uniform[0, reorder_depth], then a stable sort by key
// reorders within the jitter window. This bounds the backward displacement of any
// datagram to reorder_depth (the representative bounded-reorder UDP model — a heavy-
// tailed lingering model would have no finite window survive it and is not what a
// control-loop link exhibits).
std::vector<std::uint64_t> reordered_schedule(std::size_t count, std::size_t reorder_depth, std::uint64_t seed)
{
    lcg rng(seed);
    std::vector<std::pair<std::uint64_t, std::uint64_t>> keyed;
    keyed.reserve(count);
    for(std::size_t i = 0; i < count; ++i)
        keyed.emplace_back(static_cast<std::uint64_t>(i) + rng.bounded(reorder_depth + 1),
                           static_cast<std::uint64_t>(i));
    std::stable_sort(keyed.begin(), keyed.end(),
                     [](const auto &a, const auto &b) { return a.first < b.first; });
    std::vector<std::uint64_t> arrival;
    arrival.reserve(count);
    for(const auto &k : keyed)
        arrival.push_back(k.second);
    return arrival;
}

// Run one window width over the schedule; return the false-reject count (a fresh
// sequence rejected as too-old). Each sequence appears exactly once, so any
// reject_replay would be a logic error (asserted absent), and reject_old is the false
// reject we measure.
template <std::size_t Width>
std::size_t false_rejects(const std::vector<std::uint64_t> &arrival)
{
    anti_replay_window<Width> w;
    std::size_t false_old = 0;
    for(std::uint64_t seq : arrival)
    {
        const auto v = w.check_and_set(seq);
        if(v == replay_verdict::reject_old)
            ++false_old;
        REQUIRE(v != replay_verdict::reject_replay);
    }
    return false_old;
}

}

TEST_CASE("crypto.anti_replay_window_sweep records false-reject rates across candidate widths", "[crypto][anti_replay]")
{
    constexpr std::size_t count = 200000;
    constexpr std::size_t reorder_depth = 32;
    constexpr std::uint64_t seed = 0x5eed1234abcd0011ull;

    const auto arrival = reordered_schedule(count, reorder_depth, seed);

    const std::size_t fr16 = false_rejects<16>(arrival);
    const std::size_t fr32 = false_rejects<32>(arrival);
    const std::size_t fr64 = false_rejects<64>(arrival);
    const std::size_t fr128 = false_rejects<128>(arrival);

    // At a modeled reorder depth of 32: a 16-slot window is too narrow (a datagram
    // displaced past 16 slots straddles the floor → false rejects). A 32-slot window is
    // EXACTLY clean (the displacement bound equals the width), but with zero margin. A
    // 64-slot window is clean with a 2x margin at a fixed 8-byte (one uint64) bitmap
    // cost; 128 buys nothing extra over 64 at this depth.
    REQUIRE(fr16 > 0);
    REQUIRE(fr32 == 0);
    REQUIRE(fr64 == 0);
    REQUIRE(fr128 == 0);

    // A deeper-reorder cross-check: at depth 64 the 32-slot window starts false-
    // rejecting while 64 stays exactly clean — confirming 64 is the margin-bearing
    // choice over the zero-margin 32.
    const auto deeper = reordered_schedule(count, 64, seed);
    REQUIRE(false_rejects<32>(deeper) > 0);
    REQUIRE(false_rejects<64>(deeper) == 0);

    // The chosen constant is one of the swept candidates and is the smallest with a
    // margin-bearing zero-false-reject property at the modeled depth — a regression
    // that shrinks it below that property fails here.
    REQUIRE(plexus::crypto::k_anti_replay_window_bits == 64);
    REQUIRE(false_rejects<plexus::crypto::k_anti_replay_window_bits>(arrival) == 0);
}
