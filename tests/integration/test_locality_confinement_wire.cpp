#include "test_locality_confinement_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace locality_confinement_fixture;

namespace {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept
    {
        return current;
    }
    static void reset() noexcept
    {
        current = time_point{};
    }
    static void advance(duration d) noexcept
    {
        current += d;
    }
};

struct manual_policy
{
    using executor_type     = plexus::inproc::inproc_executor<manual_clock> &;
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using demand_transport = plexus::inproc::inproc_transport<manual_clock>;
using demand_engine    = plexus::io::routing_engine<manual_policy, demand_transport, manual_clock>;

}

TEST_CASE("locality confinement: a local-confined subscribe toward a tcp peer establishes NO "
          "remote path (demand gate), looped",
          "[integration][locality][confinement]")
{
    constexpr int           k_iterations = 100;
    constexpr std::uint64_t k_seed       = 0xC0FFEEu;

    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();

        plexus::inproc::inproc_bus<manual_clock>      bus;
        plexus::inproc::inproc_executor<manual_clock> ex{bus};
        demand_transport                              transport_a{ex, bus};
        demand_transport                              transport_b{ex, bus};

        plexus::log::null_logger sink;
        demand_engine            a(transport_a, ex, make_cfg(0xA1), std::chrono::hours(1), forever_cfg(), k_seed, sink, /*eager=*/false);
        demand_engine            b(transport_b, ex, make_cfg(0xB2), std::chrono::hours(1), forever_cfg(), k_seed, sink, /*eager=*/false);
        a.listen({"inproc", "node-a"});
        b.listen({"inproc", "node-b"});

        // Peer R: AWARE at a "tcp" endpoint (a REMOTE tier) — but unreachable here; the
        // gate must refuse a confined demand toward it WITHOUT ever building a path.
        const plexus::node_id remote_id = make_id(0xC3);
        a.note_peer(remote_id, plexus::io::endpoint{"tcp", "10.0.0.5:9000"});

        // Peer L: a REAL reachable in-scope peer at an "inproc" endpoint (PROCESS tier),
        // the live control proving the gate admits an in-scope demand (it is not a blanket
        // refusal). node B actually listens, so an admitted demand connects.
        const plexus::node_id inproc_id = make_id(0xB2);
        a.note_peer(inproc_id, plexus::io::endpoint{"inproc", "node-b"});
        ex.drain();

        // (1) A LOCAL-confined subscription toward the REMOTE-tier peer: REFUSED before any
        //     reach/dial. No slot is built, so no dial was attempted, no path established.
        a.subscribe(remote_id, "confined.topic", locality::local);
        ex.drain();
        REQUIRE_FALSE(a.has_session(remote_id));  // no slot built for the refused demand
        REQUIRE_FALSE(a.is_connected(remote_id)); // no remote path established

        // (2) The functional control: a PROCESS-scoped subscription toward the inproc peer
        //     (whose tier IS process) is ADMITTED — it dials, handshakes, and connects.
        //     The gate refuses only an out-of-scope mask, never all demand.
        a.subscribe(inproc_id, "open.topic", locality::process);
        ex.drain();
        REQUIRE(a.is_connected(inproc_id)); // an in-scope demand established a real path

        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
