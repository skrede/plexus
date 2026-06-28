#include "test_discovery_noalloc_common.h"

#include "support/alloc_counter.h"

#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/multicast_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>

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
    sock.replay(source);
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
            sock.replay(source);
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
