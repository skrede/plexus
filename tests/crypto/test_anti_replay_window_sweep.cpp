// over-limit: one cohesive empirical anti-replay-width sweep; both cells share the one
// fixed-seed LCG + reordered-schedule + reference-window harness, and that shared sweep
// fixture dominates the file, so splitting the two cells scatters it into near-empty shells.
// The recorded sweep substantiating k_anti_replay_window_bits at FRAGMENT scale. A
// large (4 MiB) message fragments into ~3500 sealed datagrams, each a distinct AEAD
// sequence; the window must admit every fragment that arrives within the link's
// reorder displacement or the too-old verdict false-rejects a fragment and the whole
// message is lost. This models a UDP link that reorders datagrams within a bounded
// displacement (the in-flight window) and runs each candidate width over the SAME
// deterministic (fixed-seed, RNG-free) arrival schedule, counting false rejects.
//
// The modeled reorder depth is anchored to the in-flight datagram window (BDP / MTU),
// the physical bound on backward displacement: ~177 on an untuned loopback
// (rmem_default 208 KiB / 1200 B) and ~1042 on a 1 Gbps × 10 ms-RTT link (the high-BDP
// realistic-link regime). Exercising the candidate grid {64, 256, 1024, 4096}: 64 and
// 256 false-reject below the realistic-link depth; 1024 is clean to ~1031, just short of
// the ~1042 realistic-link worst case; 4096 is the smallest width that bears margin over
// it. A regression that shrinks the chosen width below its no-false-reject property
// fails the asserted check at the bottom. The sweep is
// deterministic, so two back-to-back runs produce identical results (no CPU-exclusivity
// caveat applies — the metric is a pure-computation false-reject count, not a timing).

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
    explicit lcg(std::uint64_t seed) noexcept
            : m_state(seed)
    {
    }
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
std::vector<std::uint64_t> reordered_schedule(std::size_t count, std::size_t reorder_depth,
                                              std::uint64_t seed)
{
    lcg                                                  rng(seed);
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
template<std::size_t Width>
std::size_t false_rejects(const std::vector<std::uint64_t> &arrival)
{
    anti_replay_window<Width> w;
    std::size_t               false_old = 0;
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

// A bit-by-bit reference of the sliding window: the same contract as anti_replay_window
// but with the pre-optimization slide (each new bit b draws from old bit b - by). The
// word-shift slide() must produce the SAME verdict as this reference for every sequence,
// which pins it byte-identical across every by displacement.
template<std::size_t Width>
class reference_window
{
public:
    replay_verdict check_and_set(std::uint64_t seq)
    {
        if(!m_seen_any)
        {
            m_seen_any = true;
            m_highest  = seq;
            m_bits[0]  = true;
            return replay_verdict::accept;
        }
        if(seq > m_highest)
        {
            slide(seq - m_highest);
            m_highest = seq;
            m_bits[0] = true;
            return replay_verdict::accept;
        }
        const std::uint64_t back = m_highest - seq;
        if(back >= Width)
            return replay_verdict::reject_old;
        if(m_bits[back])
            return replay_verdict::reject_replay;
        m_bits[back] = true;
        return replay_verdict::accept;
    }

private:
    void slide(std::uint64_t by)
    {
        if(by >= Width)
        {
            m_bits.assign(Width, false);
            return;
        }
        for(std::size_t b = Width; b-- > 0;)
            m_bits[b] = (b >= by) && m_bits[b - by];
    }

    std::vector<bool> m_bits = std::vector<bool>(Width, false);
    std::uint64_t     m_highest{0};
    bool              m_seen_any{false};
};

// Drive the production window and the bit-by-bit reference through the SAME schedule;
// the word-shift is byte-identical iff every verdict matches.
template<std::size_t Width>
bool verdicts_match(const std::vector<std::uint64_t> &arrival)
{
    anti_replay_window<Width> w;
    reference_window<Width>   ref;
    for(std::uint64_t seq : arrival)
        if(w.check_and_set(seq) != ref.check_and_set(seq))
            return false;
    return true;
}

TEST_CASE("crypto.anti_replay_window word-shift slide is bitmap-identical to the bit-by-bit "
          "reference",
          "[crypto][anti_replay]")
{
    // A schedule spanning the displacement classes the word-shift must get right:
    // by % 64 == 0 (pure word move, the shift-by-64 edge), by % 64 != 0 (intra-word
    // carry), by < 64, by > 64, by == Width - 1, and by >= Width (the fill-0 fast path).
    const std::array<std::uint64_t, 18> by_classes{1,
                                                   63,
                                                   64,
                                                   65,
                                                   127,
                                                   128,
                                                   200,
                                                   256,
                                                   1000,
                                                   1024,
                                                   1025,
                                                   plexus::crypto::k_anti_replay_window_bits - 1,
                                                   plexus::crypto::k_anti_replay_window_bits,
                                                   plexus::crypto::k_anti_replay_window_bits + 1,
                                                   7,
                                                   33,
                                                   4095,
                                                   8192};

    std::vector<std::uint64_t> arrival;
    std::uint64_t              highest = 0;
    for(std::uint64_t by : by_classes)
    {
        highest += by;
        arrival.push_back(highest); // a forward advance that drives slide(by)
        if(highest >= 5)
        {
            arrival.push_back(highest - 1); // an in-window fresh slot
            arrival.push_back(highest - 5); // another, after the advance
            arrival.push_back(highest - 1); // a replay of an already-set slot
        }
    }

    REQUIRE(verdicts_match<64>(arrival));
    REQUIRE(verdicts_match<256>(arrival));
    REQUIRE(verdicts_match<1024>(arrival));
    REQUIRE(verdicts_match<plexus::crypto::k_anti_replay_window_bits>(arrival));

    // Also pin the reordered sweep schedule (the same arrival the reject-set sweep uses)
    // across the candidate widths — a verdict divergence anywhere fails the identity.
    const auto reordered = reordered_schedule(3500, 1042, 0x5eed1234abcd0011ull);
    REQUIRE(verdicts_match<64>(reordered));
    REQUIRE(verdicts_match<256>(reordered));
    REQUIRE(verdicts_match<1024>(reordered));
    REQUIRE(verdicts_match<plexus::crypto::k_anti_replay_window_bits>(reordered));
}

TEST_CASE("crypto.anti_replay_window_sweep records fragment-scale false-reject rates across "
          "candidate widths",
          "[crypto][anti_replay]")
{
    // A 4 MiB message at the 1200-byte MTU fragments into ~3500 sealed datagrams.
    constexpr std::size_t   count = 3500;
    constexpr std::uint64_t seed  = 0x5eed1234abcd0011ull;

    // The loopback in-flight window: rmem_default (208 KiB) / 1200 B MTU ≈ 177 datagrams
    // can be buffered at once, so backward displacement on a loopback burst is bounded
    // by ~177. The 64-slot window false-rejects here; 256 (and up) is clean.
    constexpr std::size_t loopback_depth = 177;
    const auto            loopback       = reordered_schedule(count, loopback_depth, seed);

    REQUIRE(false_rejects<64>(loopback) > 0); // too narrow even for loopback reorder
    REQUIRE(false_rejects<256>(loopback) == 0);
    REQUIRE(false_rejects<1024>(loopback) == 0);
    REQUIRE(false_rejects<4096>(loopback) == 0);

    // The realistic-link regime: a 1 Gbps × 10 ms-RTT link (BDP ~1.25 MB) keeps ~1042
    // datagrams in flight, so reorder displacement reaches ~1042. The 256-slot window
    // false-rejects (it would drop the whole 4 MiB message on such a link); the 1024-slot
    // window is clean only to ~1031, so it too false-rejects at the full realistic-link
    // depth — confirming 4096 is the smallest swept width that bears margin over it.
    constexpr std::size_t realistic_link_depth = 1042;
    const auto            realistic = reordered_schedule(count, realistic_link_depth, seed);

    REQUIRE(false_rejects<64>(realistic) > 0);
    REQUIRE(false_rejects<256>(realistic) > 0); // narrower than the realistic-link in-flight window
    REQUIRE(false_rejects<1024>(realistic) > 0);  // clean only to ~1031, just short of ~1042
    REQUIRE(false_rejects<4096>(realistic) == 0); // the margin-bearing winner

    // The chosen constant is one of the swept candidates and is the smallest clean at the
    // realistic-link reorder depth — a regression that shrinks it below that property
    // (e.g. to 1024, which false-rejects at the full realistic-link in-flight window)
    // fails here.
    REQUIRE(plexus::crypto::k_anti_replay_window_bits == 4096);
    REQUIRE(false_rejects<plexus::crypto::k_anti_replay_window_bits>(realistic) == 0);
    REQUIRE(false_rejects<plexus::crypto::k_anti_replay_window_bits>(loopback) == 0);
}
