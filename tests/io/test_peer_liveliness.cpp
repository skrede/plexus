// The peer-liveliness fusion matrix: drives the clock-free arbiter with hand-fed monotonic
// timestamps (no engine, no clock) and pins each combine policy's alive/lost/no-verdict outcome
// and the contributing bitmask over induced signal combinations. A broken fuse (OR where AND is
// meant) fails the all_required present-signal-dead case. Header-only core, linked against
// plexus::core + Catch2's main only; its sibling compiles the precedence/hysteresis cases.

#include "test_peer_liveliness_common.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>

TEST_CASE("peer_liveliness any_signal_alive fuses each signal alive with its own bit", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    h.arb.note_heartbeat(id_of(1), k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.note_awareness(id_of(2), k_base_ns);
    h.arb.evaluate(k_base_ns);
    h.arb.note_session_up(id_of(3));
    h.arb.evaluate(k_base_ns);

    REQUIRE(h.log.size() == 3);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
    REQUIRE(h.log[0].contributing == liveliness_signal::heartbeat);
    REQUIRE(h.log[1].contributing == liveliness_signal::awareness);
    REQUIRE(h.log[2].contributing == liveliness_signal::session);
}

TEST_CASE("peer_liveliness any_signal_alive unions the bits of every live signal", "[io][peer_liveliness]")
{
    harness h(combine::any_signal_alive);
    const node_id id = id_of(7);
    h.arb.note_awareness(id, k_base_ns);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.note_session_up(id);
    h.arb.evaluate(k_base_ns);

    REQUIRE(h.log.size() == 1);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
    REQUIRE(h.log[0].contributing
            == (liveliness_signal::awareness | liveliness_signal::heartbeat | liveliness_signal::session));
}

TEST_CASE("peer_liveliness session_authoritative keeps the table empty until a session edge", "[io][peer_liveliness]")
{
    harness h(combine::session_authoritative);
    h.arb.note_heartbeat(id_of(1), k_base_ns);
    h.arb.note_awareness(id_of(1), k_base_ns);
    h.arb.evaluate(k_base_ns);

    std::size_t count = 0;
    h.arb.for_each_peer([&](const node_id &, const auto &) { ++count; });
    REQUIRE(count == 0);
    REQUIRE(h.log.empty());

    h.arb.note_session_up(id_of(1));
    h.arb.evaluate(k_base_ns);
    REQUIRE(h.log.size() == 1);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
    REQUIRE(h.log[0].contributing == liveliness_signal::session);
}

TEST_CASE("peer_liveliness session_authoritative reads a session drop as lost", "[io][peer_liveliness]")
{
    harness h(combine::session_authoritative);
    h.arb.note_session_up(id_of(1));
    h.arb.evaluate(k_base_ns);
    h.arb.note_session_down(id_of(1), k_base_ns + k_interval_ns);

    REQUIRE(h.log.size() == 2);
    REQUIRE(h.log[1].verdict == liveliness_verdict::lost);
    REQUIRE(h.log[1].contributing == liveliness_signal::session);
}

TEST_CASE("peer_liveliness all_required stands on a live signal without the absent ones", "[io][peer_liveliness]")
{
    harness h(combine::all_required);
    h.arb.note_session_up(id_of(1));
    h.arb.evaluate(k_base_ns);

    REQUIRE(h.log.size() == 1);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
    REQUIRE(h.log[0].contributing == liveliness_signal::session);
}

TEST_CASE("peer_liveliness all_required reads lost when a present signal is dead", "[io][peer_liveliness]")
{
    harness h(combine::all_required);
    const node_id id = id_of(1);
    h.arb.note_session_up(id);
    h.arb.note_heartbeat(id, k_base_ns);
    h.arb.evaluate(k_base_ns);                    // session up + heartbeat fresh -> alive
    h.arb.evaluate(k_base_ns + k_window_ns + 1);  // heartbeat dead, session still up -> lost

    REQUIRE(h.log.size() == 2);
    REQUIRE(h.log[0].verdict == liveliness_verdict::alive);
    REQUIRE(h.log[1].verdict == liveliness_verdict::lost);
    REQUIRE(h.log[1].contributing == liveliness_signal::heartbeat);
}
