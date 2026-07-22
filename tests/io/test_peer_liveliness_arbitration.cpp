// The peer-liveliness precedence and hysteresis cases: transport-drop dominance with the post-drop
// staleness guard, N-miss hysteresis, no-flap under single-signal loss, the transition latch,
// subscriber gating (the latch updates while emission stays silent), and the fixed twin's
// fail-closed refusal on over-capacity. Compiles into the test_peer_liveliness target beside the
// fusion matrix; Header-only core, linked against plexus::core + Catch2's main only.

#include "test_peer_liveliness_common.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstdint>
#include <algorithm>
#include <stdexcept>

TEST_CASE("peer_liveliness a transport drop dominates a fresh heartbeat the same turn", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id = id_of(1);
    h.arb.note_session_up(id);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.note_session_down(id, k_base_ns + k_interval_ns); // heartbeat still inside the window

    REQUIRE(h.log.size() == 2);
    REQUIRE(h.log[1].verdict == liveliness_verdict::lost);
    REQUIRE(h.log[1].contributing == liveliness_signal::session);
}

TEST_CASE("peer_liveliness keeps an aware peer alive after a drop under any_signal_alive", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id = id_of(1);
    h.arb.note_awareness(id, k_base_ns);
    h.arb.note_session_up(id);
    h.arb.evaluate(k_base_ns);
    h.arb.note_session_down(id, k_base_ns + k_interval_ns);

    REQUIRE(h.log.size() == 1);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
}

TEST_CASE("peer_liveliness re-asserts alive only on heartbeat evidence newer than the drop", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id         = id_of(1);
    const std::uint64_t drop = k_base_ns + k_interval_ns;
    h.arb.note_session_up(id);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.note_session_down(id, drop);

    h.arb.note_heartbeat(id, drop - 1); // stamped before the drop: cannot vote
    h.arb.evaluate(drop + 1);
    REQUIRE(h.log.size() == 2);

    h.arb.note_heartbeat(id, drop + k_interval_ns); // newer than the drop: votes
    h.arb.evaluate(drop + k_interval_ns);
    REQUIRE(h.log.size() == 3);
    REQUIRE(h.log[2].verdict == liveliness_verdict::alive);
    REQUIRE(h.log[2].contributing == liveliness_signal::heartbeat);
}

TEST_CASE("peer_liveliness holds alive until the heartbeat misses past the window", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id = id_of(1);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.evaluate(k_base_ns + k_window_ns);     // at the window edge: still fresh
    REQUIRE(h.log.size() == 1);
    h.arb.evaluate(k_base_ns + k_window_ns + 1); // one past: dead
    REQUIRE(h.log.size() == 2);
    REQUIRE(h.log[1].verdict == liveliness_verdict::lost);
    REQUIRE(h.log[1].contributing == liveliness_signal::heartbeat);
}

TEST_CASE("peer_liveliness does not flip while another signal stays alive", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id = id_of(1);
    h.arb.note_awareness(id, k_base_ns);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.evaluate(k_base_ns + k_window_ns + 1); // heartbeat dead, awareness still alive

    REQUIRE(h.log.size() == 1);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
}

TEST_CASE("peer_liveliness latches so a repeated evaluate emits at most once", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id = id_of(1);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.evaluate(k_base_ns + 1);
    h.arb.evaluate(k_base_ns + 2);

    REQUIRE(h.log.size() == 1);
}

TEST_CASE("peer_liveliness updates the latch but emits nothing without a subscriber", "[io][peer_liveliness]")
{
    peer_liveliness<> arb(opts_for(combine::any_signal_alive));
    std::vector<peer_liveliness_event> log;
    arb.on_verdict([&](const peer_liveliness_event &e) { log.push_back(e); });

    arb.note_heartbeat(id_of(1), k_base_ns);
    arb.evaluate(k_base_ns); // latch -> alive, but unsubscribed
    REQUIRE(log.empty());

    arb.add_subscriber();
    arb.evaluate(k_base_ns + 1); // unchanged from the latched alive -> still no emission
    REQUIRE(log.empty());
}

// A via-only reported identity is never fed to the direct-peer arbiter (routing_engine's
// note_reported_candidate takes no arbiter path), so it can never draw a verdict. Pinned here at the
// arbiter surface: a peer given no signal is absent from the verdict log even as a directly-fed peer
// churns through alive/lost — the reachability derivation reads the arbiter, it never feeds it.
TEST_CASE("peer_liveliness a reported-only identity never fed a signal never receives a verdict", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id direct   = id_of(1);
    const node_id reported = id_of(2);

    h.arb.note_session_up(direct);
    h.arb.evaluate(k_base_ns);
    h.arb.note_session_down(direct, k_base_ns + k_interval_ns);
    h.arb.evaluate(k_base_ns + k_window_ns + 1);

    REQUIRE(std::any_of(h.log.begin(), h.log.end(), [&](const peer_liveliness_event &e) { return e.id == direct; }));
    REQUIRE(std::none_of(h.log.begin(), h.log.end(), [&](const peer_liveliness_event &e) { return e.id == reported; }));
}

TEST_CASE("peer_liveliness fixed twin fails closed on the (N+1)-th distinct peer", "[io][peer_liveliness]")
{
    peer_liveliness<fixed_liveliness_peer_storage<2>> arb(opts_for(combine::any_signal_alive));
    arb.note_heartbeat(id_of(1), k_base_ns);
    arb.note_heartbeat(id_of(2), k_base_ns);
    REQUIRE_THROWS_AS(arb.note_heartbeat(id_of(3), k_base_ns), std::runtime_error);
}
