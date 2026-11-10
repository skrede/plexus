#include "test_node_discovery_aging_common.h"

using namespace node_discovery_aging_fixture;

TEST_CASE("node_discovery_aging: a peer that stops announcing ages out of the node's known() within TTL", "[node][discovery_aging]")
{
    test_clock::reset();
    wired_pair p;
    const auto announcer_id = make_id(0xB2);
    const endpoint ep       = ep_of("announcer:6000");

    // The announcer's card lands in the observer's awareness (the discovery-to-known() feed).
    p.observer.note_peer(announcer_id, ep, clock_now_ns());
    REQUIRE(p.observer.known().contains(announcer_id));

    // It keeps re-announcing for a while: it stays known across the span.
    for(int i = 0; i < 5; ++i)
    {
        p.advance(k_ttl / 2);
        p.observer.note_peer(announcer_id, ep, clock_now_ns());
        REQUIRE(p.observer.known().contains(announcer_id));
    }

    // Now it goes silent (no re-announce, no session/heartbeat): aged out within the TTL.
    p.advance(k_ttl + k_tick_granularity);
    REQUIRE_FALSE(p.observer.known().contains(announcer_id));
}
