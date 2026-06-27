#include "test_known_peers_aging_common.h"

using namespace known_peers_aging_fixture;

TEST_CASE("known_peers_aging: a peer that stops re-announcing ages out of awareness at the TTL", "[io][known_peers_aging]")
{
    test_clock::reset();
    aging_pair p;
    const auto id_b = make_id(0xB2);
    const endpoint ep{"inproc", "svc"};

    p.a.note_peer(id_b, ep, clock_now_ns());
    REQUIRE(p.a.known().contains(id_b));

    // Short of the TTL: still known (the sweep finds it fresh).
    p.advance(k_ttl / 2 + k_tick_granularity);
    REQUIRE(p.a.known().contains(id_b));

    // Past the TTL with no re-announce and no session: swept out of awareness.
    p.advance(k_ttl + k_tick_granularity);
    REQUIRE_FALSE(p.a.known().contains(id_b));
}

TEST_CASE("known_peers_aging: a re-announce refreshes the timestamp and defers expiry", "[io][known_peers_aging]")
{
    test_clock::reset();
    aging_pair p;
    const auto id_b = make_id(0xB2);
    const endpoint ep{"inproc", "svc"};

    p.a.note_peer(id_b, ep, clock_now_ns());

    // Just under the TTL, a fresh re-announce resets the clock.
    p.advance(k_ttl - k_tick_granularity);
    p.a.note_peer(id_b, ep, clock_now_ns());
    REQUIRE(p.a.known().contains(id_b));

    // Another near-TTL span: it survives because the re-announce restamped it.
    p.advance(k_ttl - k_tick_granularity);
    REQUIRE(p.a.known().contains(id_b));
}
