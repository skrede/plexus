#include "test_dialed_identity_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/io/detail/routing_session_scan.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace dialed_identity_fixture;

namespace {

constexpr std::string_view k_topic = "/dialed/topic";

struct lost_verdicts final : plexus::io::observer
{
    explicit lost_verdicts(const plexus::node_id &id)
            : lost(0)
            , watched(id)
    {
    }

    bool observes_liveliness() const override
    {
        return true;
    }

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &ev) override
    {
        if(ev.id == watched && ev.verdict == plexus::io::liveliness_verdict::lost)
            ++lost;
    }

    std::int32_t lost;
    plexus::node_id watched;
};

}

TEST_CASE("dialed identity binding: a rejection keeps the peer's awareness and the graph generation", "[integration][dialed_identity]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.a.add_observer(rec);
    const auto stale = make_id(0xC3);
    net.a.note_peer(stale, net.ep_b);
    const auto generation = net.a.graph_generation();
    net.a.reach(stale);
    net.drive();
    REQUIRE(rec.for_peer(stale).rejected == 1);
    CHECK(net.a.graph_generation() == generation);
    CHECK(net.a.known().contains(stale));
    CHECK(net.a.known().lookup(stale) == net.ep_b);
    CHECK(net.a.known().candidates(stale).size() == 1);
}

TEST_CASE("dialed identity binding: a rejection does not turn the peer's liveliness verdict lost", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    lost_verdicts probe(t.advertised);
    t.net.a.add_observer(probe);
    for(std::int32_t tick = 0; tick < 5; ++tick)
        t.net.advance(std::chrono::milliseconds(100));
    CHECK(probe.lost == 0);
    t.net.a.remove_observer(probe);
}

TEST_CASE("dialed identity binding: a card for the identity now at the endpoint does not re-authorize the stale card", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    t.net.a.note_peer(t.net.id_b, t.net.ep_b);
    t.net.a.reach(t.net.id_b);
    t.net.drive();
    REQUIRE(t.net.a.is_connected(t.net.id_b));
    t.net.a.subscribe(t.advertised, k_topic);
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == 1);
    CHECK(t.net.a.is_connected(t.net.id_b));
    CHECK(t.rec.for_peer(t.net.id_b).disconnected == 0);
}

TEST_CASE("dialed identity binding: a session proving the id serves demand and its end does not redial the stale endpoint", "[integration][dialed_identity]")
{
    manual_clock::reset();
    misdirected_card t;
    REQUIRE(t.rec.for_peer(t.advertised).rejected == 1);
    transport_t transport_c{t.net.ex, t.net.bus};
    engine c(transport_c, t.net.ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, t.net.sink, false);
    c.dial(t.net.ep_a);
    t.net.drive();
    t.net.a.subscribe(t.advertised, k_topic);
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == 1);
    CHECK(plexus::io::detail::live_session_for(t.net.a, t.advertised) != nullptr);
    c.hang_up(t.net.ep_a);
    t.net.drive();
    t.net.a.subscribe(t.advertised, k_topic);
    t.net.drive();
    CHECK(t.rec.for_peer(t.advertised).rejected == 1);
}

TEST_CASE("dialed identity binding: a slot rejected for its identity is erased once drained and the next card dials through a fresh slot", "[integration][dialed_identity]")
{
    manual_clock::reset();
    two_node net;
    recording_observer rec;
    net.a.add_observer(rec);
    const auto stale        = make_id(0xC3);
    const std::size_t slots = net.a.registry().slot_count();
    net.a.note_peer(stale, net.ep_b);
    net.a.reach(stale);
    net.drive();
    REQUIRE(rec.for_peer(stale).rejected == 1);
    CHECK(net.a.registry().slot_count() == slots);

    net.a.subscribe(stale, k_topic);
    net.drive();
    CHECK(rec.for_peer(stale).rejected == 1);
    CHECK(net.a.registry().slot_count() == slots);

    net.a.note_peer(stale, net.ep_b);
    net.a.reach(stale);
    net.drive();
    CHECK(rec.for_peer(stale).rejected == 2);
    CHECK(net.a.registry().slot_count() == slots);
}
