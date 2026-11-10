#include "test_discovery_noalloc_common.h"

#include "support/alloc_counter.h"

#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/multicast_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <algorithm>

using namespace discovery_noalloc_fixture;

TEST_CASE("discovery re-announce is allocation-free; the inbound resolve build is a bounded "
          "per-call constant",
          "[integration][discovery]")
{
    // The discovery steady-state path has two measured segments over a non-allocating sink Policy:
    //
    //   (A) the re-announce path — the re-announce timer fires emit_announcement(), which re-encodes
    //       the announcement cached at advertise() into the reused m_scratch (no rebuild of the
    //       listens, no per-emit alloc once warm) and re-arms the timer. The cached announcement is
    //       what makes a sustained announce stream the zero-allocation steady-state path.
    //
    //   (B) the inbound resolve — on_inbound decodes the datagram and builds a service_info via
    //       service_info_from_announcement (a listens vector + assemble_contact_card's metadata and
    //       its per-key strings). That build legitimately allocates a small bounded set of blocks
    //       per call; it is identity construction, NOT a buffer the path can reuse, so it is
    //       measured HONESTLY as a fixed per-call constant that does not grow with the loop count K
    //       — the same posture the inproc rpc-dispatch gate sets for its correlation table.
    sink_executor ex;
    sink_datagram_socket sock;

    // per_source_max == 0 disables the rate window so every inbound resolve fires; the first inbound
    // emplaces the source into the admission map (a one-time setup alloc warmed away), and every
    // later one only refreshes that entry — isolating segment (B) to the service_info build.
    plexus::discovery::discovery_options opts;
    opts.cap.per_source_max = 0;

    plexus::discovery::multicast_discovery<sink_datagram_socket, sink_policy> disco{ex, sock, opts};

    const auto card          = make_card();
    const auto peer          = make_peer_datagram();
    const std::string source = "127.0.1.7";

    int resolved = 0;
    disco.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    // Warm-up: advertise() caches the announcement, grows m_scratch and the recorded buffer, and
    // arms the timer; one inbound resolve grows the service_info build's blocks and the admission
    // entry. The re-announce path proper is the timer fire below.
    disco.advertise(card);
    REQUIRE(!sock.last.empty());
    REQUIRE(ex.armed != nullptr);
    ex.armed->fire();
    sock.replay_bytes(source, peer);
    REQUIRE(resolved == 1);

    constexpr int K = 1000;

    // Segment (A): K timer-driven re-announces of the cached card. The scratch is warm, the
    // announcement is cached, and the re-armed timer callback lives in the SBO, so the delta must
    // be a clean 0.
    {
        const int sends_before = sock.sends;
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            ex.armed->fire();
        const auto after = plexus::testing::alloc_count();
        REQUIRE(sock.sends - sends_before == K); // every re-announce emitted
        REQUIRE(after - before == 0);            // encode + send + re-arm: ZERO steady-state alloc
    }

    // Segment (B): the inbound resolve build. Measured at two loop counts: doubling the loop doubles
    // the allocations exactly (so the cost scales with calls, not throughput) and the per-call
    // constant is small and bounded — not a forced 0, since the service_info build legitimately
    // constructs a fresh card.
    auto inbound_round_allocs = [&](int n) -> std::size_t
    {
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < n; ++i)
            sock.replay_bytes(source, peer);
        return plexus::testing::alloc_count() - before;
    };

    const int resolved_before   = resolved;
    const std::size_t allocs_k  = inbound_round_allocs(K);
    const std::size_t allocs_2k = inbound_round_allocs(2 * K);
    REQUIRE(resolved - resolved_before == K + 2 * K); // every inbound resolved

    REQUIRE(allocs_2k == 2 * allocs_k);
    REQUIRE(allocs_k % static_cast<std::size_t>(K) == 0);
    const std::size_t per_call = allocs_k / static_cast<std::size_t>(K);
    REQUIRE(per_call >= 1);  // building a fresh service_info is not free
    REQUIRE(per_call <= 12); // but it is a small bounded constant, not a growth with K
}

TEST_CASE("discovery drops a foreign universe allocation-free before the resolve build",
          "[integration][discovery]")
{
    // The inbound universe compare returns before service_info_from_announcement's bounded build,
    // before from.address().to_string(), and before flood_cap.admit — so a foreign-universe
    // datagram pays nothing. The injected announcement carries ZERO listens, so decode_announcement
    // itself reserves nothing (it grows the listens vector only when listens are present): the drop
    // is then the u32 compare alone and the measured delta is a literal 0.
    sink_executor ex;
    sink_datagram_socket sock;

    plexus::discovery::discovery_options opts;
    plexus::discovery::multicast_discovery<sink_datagram_socket, sink_policy> disco{ex, sock, opts};

    int resolved = 0;
    disco.browse([&](const plexus::discovery::service_info &) { ++resolved; });

    plexus::node_id fid{};
    fid[0]  = std::byte{0x91};
    fid[15] = std::byte{0x6e};
    const std::uint32_t foreign = opts.universe ^ 0xFFFFFFFFu;
    const auto bytes            = plexus::wire::encode_announcement(
            plexus::discovery::detail::announcement_from_card(fid, {}, 30, 0, foreign));

    const std::string source = "127.0.2.9";
    sock.replay_bytes(source, bytes); // warm the handler path once
    REQUIRE(resolved == 0);

    constexpr int K = 1000;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        sock.replay_bytes(source, bytes);
    const auto after = plexus::testing::alloc_count();

    REQUIRE(resolved == 0);       // never admitted
    REQUIRE(after - before == 0); // the compare returns before every allocating step
}

TEST_CASE("discovery re-announce stays allocation-free with announce jitter engaged, and the "
          "jittered interval genuinely varies",
          "[integration][discovery]")
{
    // The re-announce cadence is now decorrelated: arm_timer draws next = period - U(0, fraction *
    // period) from a member-owned mt19937 (seeded from the node id at advertise). This gate proves
    // the jittered arm churns ZERO at steady state AND that the interval is genuinely jittered — not
    // a constant masquerading as jitter — so a regression that either allocates in the draw or
    // silently disables the jitter fails here.
    sink_executor ex;
    sink_datagram_socket sock;

    plexus::discovery::discovery_options opts;
    opts.cap.per_source_max = 0;
    REQUIRE(opts.jitter_fraction > 0.0); // the default arm is jittered, not phase-locked

    plexus::discovery::multicast_discovery<sink_datagram_socket, sink_policy> disco{ex, sock, opts};
    disco.browse([](const plexus::discovery::service_info &) {});
    disco.advertise(make_card()); // seeds the per-node RNG and arms the jittered timer
    REQUIRE(ex.armed != nullptr);

    constexpr int K              = 1000;
    const std::int64_t period    = opts.announce_period.count();
    const std::int64_t max_span  = static_cast<std::int64_t>(opts.jitter_fraction * static_cast<double>(period));

    // Warm one tick so the reused callback slot and the send scratch are at steady state.
    ex.armed->fire();

    std::vector<std::int64_t> delays;
    delays.reserve(K); // grown ONCE before the measured window so the loop's push_back never allocs

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
    {
        ex.armed->fire();
        delays.push_back(ex.armed->last_delay.count());
    }
    const auto after = plexus::testing::alloc_count();

    REQUIRE(after - before == 0); // encode + send + jittered re-arm: ZERO steady-state alloc

    for(const std::int64_t d : delays)
    {
        REQUIRE(d >= period - max_span); // never below the floor
        REQUIRE(d <= period);            // never above the nominal period
        REQUIRE(d >= 1);                 // clamped strictly positive
    }
    // The sequence is not constant: at least one pair of successive intervals differs.
    REQUIRE(std::adjacent_find(delays.begin(), delays.end(), std::not_equal_to<>{}) != delays.end());
}
