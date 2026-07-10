// The fused peer-liveliness precedence + hysteresis oracle, end-to-end through the inproc engine on
// the deterministic virtual clock: a real transport drop dominates a still-fresh heartbeat and reads
// lost within one tick (not after a lease that never fires); an awareness loss while a session keeps
// heartbeating does NOT flap the verdict; post-drop silence stays lost and a stale pre-drop heartbeat
// never resurrects the peer; and a rejected handshake settles the peer lost under
// session_authoritative rather than leaving it stuck-alive. Every drop is a REAL partner-end close on
// the production receive path (no injection), the clock is advanced by hand, and the step-executor is
// drained — never a wall-clock sleep.

#include "liveliness_inproc_common.h"

#include "plexus/log/logger.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

using namespace liveliness_inproc;

namespace {

constexpr auto alive = liveliness_verdict::alive;
constexpr auto lost  = liveliness_verdict::lost;

handshake_fsm_config cfg_for(std::uint8_t id_seed, std::uint8_t compatible)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = compatible, .compatible_version_minor = 0};
}

// A observing engine and a listening peer B over one bus. B lives in an optional so a scenario can
// destroy it to make a re-dial fail (proving the peer stays lost with no confounding reconnect). A's
// compatible-version floor is selectable so the rejection leg can force a version-incompatible refusal.
struct arb_net
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};
    plexus::log::null_logger sink;
    recording_liveliness_observer obs;
    engine a;
    std::optional<engine> b;
    endpoint ep_b{"inproc", "node-b"};
    plexus::node_id slot_b{endpoint_id(ep_b)};

    explicit arb_net(combine policy, std::uint8_t a_compatible = 1)
            : a(transport_a, ex, cfg_for(0xA1, a_compatible), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes,
                opts(policy))
    {
        b.emplace(transport_b, ex, cfg_for(0xB2, 1), k_long_timeout, forever_cfg(), k_seed + 1, sink, false);
        b->listen(ep_b);
        a.add_observer(obs);
    }

    void drive()
    {
        ex.drain();
    }
    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        ex.drain();
    }

    // A REAL transport drop: close B's accepted end so A observes on_error from its own receive path.
    void drop_b()
    {
        auto *inbound = b->session_for(inbound_slot(1));
        REQUIRE(inbound != nullptr);
        inbound->tear_down();
        ex.drain();
    }
};

}

TEST_CASE("liveliness.arbitration: a transport drop while heartbeating reads lost within one tick", "[integration][liveliness][arbitration]")
{
    manual_clock::reset();
    arb_net net{combine::any_signal_alive};

    // Dialed with NO awareness note: the peer is alive by session + heartbeat only, so a drop clears
    // every live signal (awareness would deliberately outlive a drop until the TTL under this policy).
    net.a.dial(net.ep_b);
    net.drive();
    REQUIRE(net.a.is_connected(net.slot_b));

    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);
    REQUIRE(net.obs.count(lost) == 0);

    // A REAL partner-end close: the dialer's dead session is torn down on the next backoff — well
    // inside one tick — and the fused verdict reads lost then, NOT after a heartbeat lease that never
    // fires. The just-fresh heartbeat is stale-guarded by the drop stamp and cannot keep it alive.
    net.drop_b();
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(lost) == 1);
}

TEST_CASE("liveliness.arbitration: an awareness loss while a session heartbeats does not flap the verdict", "[integration][liveliness][arbitration]")
{
    manual_clock::reset();
    arb_net net{combine::any_signal_alive};
    const auto id_b = make_id(0xB2);

    net.a.note_peer(id_b, net.ep_b, clock_now_ns());
    net.a.reach(id_b);
    net.drive();
    REQUIRE(net.a.is_connected(id_b));

    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);

    // A goodbye drops awareness only; the session stays up and keeps heartbeating, so the fused
    // verdict must NOT transition to lost (no single-signal flap).
    net.a.forget(id_b);
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);
    REQUIRE(net.obs.count(lost) == 0);
}

TEST_CASE("liveliness.arbitration: post-drop silence stays lost and a stale pre-drop heartbeat does not resurrect it", "[integration][liveliness][arbitration]")
{
    manual_clock::reset();
    arb_net net{combine::any_signal_alive};

    net.a.dial(net.ep_b);
    net.drive();
    REQUIRE(net.a.is_connected(net.slot_b));
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);

    // Drop the session on the dialer's own end so it settles down with no automatic re-dial: the
    // channel closes, the drop stamp lands, and the just-taken heartbeat becomes stale evidence.
    net.a.session_for(net.slot_b)->tear_down();
    net.drive();
    REQUIRE(net.obs.count(lost) == 1);
    REQUIRE_FALSE(net.a.is_connected(net.slot_b));

    // Silence: no reconnect and no new heartbeat, so the stale pre-drop heartbeat can never re-earn
    // alive — the verdict holds lost across many ticks with no flap.
    for(int step = 0; step < 5; ++step)
        net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);
    REQUIRE(net.obs.count(lost) == 1);
}

TEST_CASE("liveliness.arbitration: a rejected handshake settles the peer lost under session_authoritative", "[integration][liveliness][arbitration]")
{
    manual_clock::reset();
    // A requires compatible >= 2 while B advertises version 1, so A refuses B's response and fires
    // rejected without ever connecting. Under session_authoritative the mapped session-down settles it.
    arb_net net{combine::session_authoritative, /*a_compatible=*/2};
    const auto id_b = make_id(0xB2);

    net.a.note_peer(id_b, net.ep_b, clock_now_ns());
    net.a.reach(id_b);
    net.drive();

    REQUIRE_FALSE(net.a.is_connected(id_b));
    REQUIRE(net.obs.count(alive) == 0);
    REQUIRE(net.obs.count(lost) == 1);
}
