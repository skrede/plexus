// The late-joiner gate over the node facade: a peer that joins AFTER the demand is in
// place still exchanges data with zero user action on join timing — the standing-demand
// acceptance, proven looped and reproducible across BOTH join orders. Each iteration
// builds fresh nodes and tears them fully down so liveness cannot leak forward (the loop
// is the reproducibility proof, not a soak). Every iteration is individually asserted; a
// stall fails on a bounded pump budget with a diagnostic, never an unbounded wait.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;
using inproc_publisher = plexus::publisher<>;
using inproc_subscriber = plexus::subscriber<>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Only the node that joins SECOND dials eagerly: it accepts the live discovery record the
// first node already advertised and dials into the single connection. Two eager nodes over
// a shared bus mutually dial into two sessions per peer (simultaneous connect), double-
// delivering every publish — single-dialer keeps exactly one connection.
plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0x1A7E10u;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// Step the executor until `done` holds or a bounded step budget is spent. The inproc
// executor is deterministic (no wall-clock dependence in this gate — there is no pending
// timer), so the budget is a step count: a converged scenario exits the instant the
// predicate holds, and a regression fails on the bound rather than hanging. step() is
// called BEFORE the predicate re-check so a one-step settle (e.g. flushing a posted fan)
// makes real progress rather than returning on the initial state.
template <typename Pred>
bool pump_until(inproc_executor<> &ex, Pred done, int step_budget = 100000)
{
    for(int i = 0; i < step_budget && !done(); ++i)
        if(!ex.step() && !done())
            return false;
    return done();
}

}

// The subscriber demand originates from the SUBSCRIBER and must reach the publisher over
// a connection the subscriber dialed — the engine's demand verbs and session_for read the
// dialed slot. So the subscriber is the eager single-dialer in BOTH orders, and what the
// late-join gate varies is the JOIN ORDER: which node is present first vs constructed and
// discovered later. The publisher is the lazy accepter (single connection per peer).

// Order 1 — a late-joining PUBLISHER reaches a subscriber whose standing demand was
// installed first. The subscriber (eager) listens and subscribes with NO peer known; the
// publisher node is then constructed and listens, the subscriber discovers it, re-dials,
// and the standing demand re-fans to the new peer — its publishes reach the callback.
TEST_CASE("late join: a publisher that joins after the subscriber's standing demand is delivered seamlessly, looped",
          "[node][late-join]")
{
    constexpr int k_iterations = 8;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex{bus};
        inproc_transport<> ta{ex, bus};
        inproc_transport<> tb{ex, bus};
        static_discovery disc{{}};

        const auto id_a = make_id(0x0A);
        const auto id_b = make_id(0x0B);

        // A: the subscriber, present first, eager (it dials the late publisher on discovery).
        inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};

        std::vector<std::string> got;
        inproc_subscriber s{a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        a.listen({"inproc", "host-a:5000"});
        ex.drain();

        // No peer is known yet — the demand is standing, with nothing to fan to.
        REQUIRE_FALSE(a.router().is_connected(id_b));

        // B joins LATE: it advertises into the live discovery record A already browses; A
        // re-dials it and the standing demand re-fans with zero user action on join timing.
        inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};
        b.listen({"inproc", "host-b:6000"});
        REQUIRE(pump_until(ex, [&] { return a.router().is_connected(id_b); }));

        inproc_publisher p{b, "topic"};
        ex.drain();

        const std::string payload = "late-pub-" + std::to_string(iter);
        p.publish(as_bytes(payload));
        REQUIRE(pump_until(ex, [&] { return !got.empty(); }));

        REQUIRE(got.size() == 1);
        REQUIRE(got.front() == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

// Order 2 — a late-joining SUBSCRIBER reaches a publisher already present. The publisher
// node listens and declares first; the subscriber (eager) then joins, discovers the live
// publisher record, dials it, installs its standing demand, and the publisher's subsequent
// publishes reach it — the reverse join order, with the subscriber still the dialer.
TEST_CASE("late join: a subscriber that joins after the publisher is already present is delivered seamlessly, looped",
          "[node][late-join]")
{
    constexpr int k_iterations = 8;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex{bus};
        inproc_transport<> ta{ex, bus};
        inproc_transport<> tb{ex, bus};
        static_discovery disc{{}};

        const auto id_a = make_id(0x0A);
        const auto id_b = make_id(0x0B);

        // B: the publisher, present first, lazy (it accepts the late subscriber's dial).
        inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};
        inproc_publisher p{b, "topic"};
        b.listen({"inproc", "host-b:6000"});
        ex.drain();

        // A joins LATE: it discovers B's live record, dials eagerly, installs its standing
        // demand, and B reacts to the in-band subscribe — no user action on join timing.
        inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
        std::vector<std::string> got;
        inproc_subscriber s{a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        a.listen({"inproc", "host-a:5000"});
        REQUIRE(pump_until(ex, [&] { return a.router().is_connected(id_b); }));
        // Let A's standing subscribe demand round-trip to B and register the fanout before
        // the publish — the connection being up precedes the wire-subscribe arriving.
        ex.drain();

        const std::string payload = "late-sub-" + std::to_string(iter);
        p.publish(as_bytes(payload));
        REQUIRE(pump_until(ex, [&] { return !got.empty(); }));

        REQUIRE(got.size() == 1);
        REQUIRE(got.front() == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
