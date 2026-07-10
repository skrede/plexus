// The fused peer-liveliness verdict edge, end-to-end through the inproc engine on the deterministic
// virtual clock: a subscribed observer sees one alive/lost truth assembled from the awareness,
// heartbeat, and session feeds; an unsubscribed node emits zero verdicts and ages exactly as the
// carried floor does (the byte-identical-floor realization). Every timing leg advances the clock by
// hand and drains the inproc step-executor — never a wall-clock sleep.

#include "liveliness_inproc_common.h"

#include "plexus/log/logger.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

using namespace liveliness_inproc;

using plexus::io::liveliness_signal;

namespace {

constexpr auto alive = liveliness_verdict::alive;
constexpr auto lost  = liveliness_verdict::lost;

bool carries(liveliness_signal mask, liveliness_signal bit)
{
    return (mask & bit) == bit;
}

// A observing engine and a listening peer B over one bus. A dials or notes B; the observer opts into
// the fused edge unless a scenario disables it to exercise the unsubscribed floor.
struct verdict_net
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};
    plexus::log::null_logger sink;
    recording_liveliness_observer obs;
    engine a;
    engine b;
    endpoint ep_b{"inproc", "node-b"};
    plexus::node_id slot_b{endpoint_id(ep_b)};

    explicit verdict_net(bool observe = true)
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes,
                opts(combine::any_signal_alive))
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed + 1, sink, false)
    {
        obs.observes = observe;
        b.listen(ep_b);
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
};

}

TEST_CASE("liveliness.verdict: a subscribed observer sees an alive edge on awareness and a lost edge on goodbye", "[integration][liveliness][verdict]")
{
    manual_clock::reset();
    verdict_net net;
    const auto id = make_id(0xCC);

    net.a.note_peer(id, endpoint{"inproc", "ghost"}, clock_now_ns());
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);
    REQUIRE(net.obs.count(lost) == 0);
    REQUIRE(carries(net.obs.events.front().contributing, liveliness_signal::awareness));

    net.a.forget(id);
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);
    REQUIRE(net.obs.count(lost) == 1);
}

TEST_CASE("liveliness.verdict: a connected heartbeating peer settles alive through the fused edge", "[integration][liveliness][verdict]")
{
    manual_clock::reset();
    verdict_net net;

    net.a.dial(net.ep_b);
    net.drive();
    REQUIRE(net.a.is_connected(net.slot_b));

    net.a.note_peer(net.slot_b, net.ep_b, clock_now_ns());
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.count(alive) == 1);
    REQUIRE(net.obs.count(lost) == 0);
    REQUIRE(carries(net.obs.events.front().contributing, liveliness_signal::session));
}

TEST_CASE("liveliness.verdict: with no liveliness subscriber the floor emits zero verdicts and ages identically", "[integration][liveliness][verdict]")
{
    manual_clock::reset();
    verdict_net net{/*observe=*/false};

    // The would-be alive/lost transitions still assemble internally, but nothing is emitted.
    const auto id = make_id(0xCC);
    net.a.note_peer(id, endpoint{"inproc", "ghost"}, clock_now_ns());
    net.advance(k_tick_granularity);
    net.a.forget(id);
    net.advance(k_tick_granularity);
    REQUIRE(net.obs.events.empty());

    // The awareness-aging floor is unchanged: a silent peer still ages out of known() at the TTL.
    const auto silent = make_id(0xDD);
    net.a.note_peer(silent, endpoint{"inproc", "ghost2"}, clock_now_ns());
    REQUIRE(net.a.known().contains(silent));
    net.advance(k_ttl + k_tick_granularity);
    REQUIRE_FALSE(net.a.known().contains(silent));
    REQUIRE(net.obs.events.empty());
}
