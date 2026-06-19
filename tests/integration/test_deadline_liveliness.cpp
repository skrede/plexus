#include "test_deadline_liveliness_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace deadline_fixture;

TEST_CASE("deadline liveliness: a data gap beyond the requested deadline fires exactly one "
          "missed-deadline")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        manual_clock::reset();
        net n;

        // A subscribes to B's topic with its OWN requested deadline (the new overload).
        n.a.subscribe(n.id_b, "topic", subscriber_qos{.requested_deadline_ns = ns_of(k_period)});
        n.drive();
        REQUIRE(n.a.is_connected(n.id_b));

        // A's subscribe already drove B's on_subscribe -> attach_for_fanout, so B fans
        // "topic" back to A. Resolve B's accepted slot to drive the publishes.
        const plexus::node_id b_inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_to_a = n.b.session_for(b_inbound);
        REQUIRE(b_to_a != nullptr);

        // A first data frame stamps the deadline clock.
        n.b.messages().publish("topic", as_bytes("d0"), b_to_a->session_id());
        n.drive();

        // A short gap (under the period) fires nothing.
        n.advance(k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 0);

        // Resume, then lapse past the period: exactly one missed-deadline.
        n.b.messages().publish("topic", as_bytes("d1"), b_to_a->session_id());
        n.drive();
        n.advance(k_period + k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 1);

        // A further tick while still lapsed fires NO second event (edge-latched).
        n.advance(k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 1);

        // Resume clears the latch; a second lapse re-fires.
        n.b.messages().publish("topic", as_bytes("d2"), b_to_a->session_id());
        n.drive();
        n.advance(k_period + k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 2);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("deadline liveliness: a heartbeat lapse beyond the lease fires exactly one lease-expiry; "
          "a continuing heartbeat keeps it alive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        manual_clock::reset();
        net n;

        // A subscribes with only a lease requested (no deadline).
        n.a.subscribe(n.id_b, "topic", subscriber_qos{.requested_lease_ns = ns_of(k_lease)});
        n.drive();
        REQUIRE(n.a.is_connected(n.id_b));

        // With the tick emitting keepalive heartbeats across the live connection,
        // advancing several lease windows must NOT expire the lease (presence asserted).
        // Step a tick at a time so each tick's heartbeat refreshes presence before the
        // gap grows — the decoupled-token semantics (alive on the heartbeat alone, no
        // data flowing).
        const int steps = 2 * static_cast<int>(k_lease / k_tick_granularity);
        for(int s = 0; s < steps; ++s)
            n.advance(k_tick_granularity);
        REQUIRE(n.count(liveness_kind::lease_expired) == 0);

        // Now silence the peer: tear B's accepted slot down so no heartbeat AND no data
        // reaches A. (A's own tick keeps firing but stamps nothing for B once silent.)
        const plexus::node_id b_inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_to_a = n.b.session_for(b_inbound);
        REQUIRE(b_to_a != nullptr);
        b_to_a->tear_down();
        n.drive();

        // A presence gap beyond the lease: exactly one lease-expiry. A's registration of
        // the B endpoint survives until A sees the disconnect; the silence is what fires.
        n.advance(k_lease + k_tick_granularity);
        const int expired = n.count(liveness_kind::lease_expired);
        REQUIRE(expired == 1);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}
