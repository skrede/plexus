#include "test_known_peers_aging_common.h"

using namespace known_peers_aging_fixture;

TEST_CASE("known_peers_aging: aging removes AWARENESS only - an active session survives", "[io][known_peers_aging]")
{
    test_clock::reset();
    aging_pair p;
    const endpoint ep{"inproc", "svc"};
    const auto slot_id = endpoint_id(ep);

    p.a.dial(ep);
    p.ex.drain();
    REQUIRE(p.a.is_connected(slot_id));

    // Drive far past the TTL: the per-tick heartbeat keeps the slot present and the sweep never
    // tears down the registry session.
    p.advance(k_ttl * 2 + k_tick_granularity);
    REQUIRE(p.a.is_connected(slot_id));
    REQUIRE(p.a.session_for(slot_id) != nullptr);

    // A separately-noted SILENT peer (no session) ages out on the SAME sweep that left the live
    // session untouched — proving the sweep is awareness-only.
    const auto silent = make_id(0xCC);
    p.a.note_peer(silent, endpoint{"inproc", "ghost"}, clock_now_ns());
    p.advance(k_ttl + k_tick_granularity);
    REQUIRE_FALSE(p.a.known().contains(silent));
    REQUIRE(p.a.is_connected(slot_id));
    REQUIRE(p.a.session_for(slot_id) != nullptr);
}

TEST_CASE("known_peers_aging: a heartbeating peer never ages out even with no re-announce", "[io][known_peers_aging]")
{
    test_clock::reset();
    aging_pair p;
    const endpoint ep{"inproc", "svc"};
    const auto slot_id = endpoint_id(ep);

    p.a.dial(ep);
    p.ex.drain();
    REQUIRE(p.a.is_connected(slot_id));

    // Record awareness for the connected slot, then go far past the TTL WITHOUT any re-announce.
    // The per-tick heartbeat from B refreshes A's awareness of the slot, so it survives.
    p.a.note_peer(slot_id, ep, clock_now_ns());
    REQUIRE(p.a.known().contains(slot_id));

    p.advance(k_ttl * 3 + k_tick_granularity);
    REQUIRE(p.a.known().contains(slot_id));
}
