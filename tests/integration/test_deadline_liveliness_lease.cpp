#include "test_deadline_liveliness_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace deadline_fixture;

TEST_CASE("deadline liveliness: a heartbeat refreshes the lease but not the deadline (the "
          "two-stamp end-to-end)")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        manual_clock::reset();
        net n;

        // Subscribe with BOTH a deadline P and a lease L (L > P): the keepalive
        // heartbeats keep presence alive while data lapses past the deadline.
        n.a.subscribe(n.id_b, "topic",
                      subscriber_qos{.requested_deadline_ns = ns_of(k_period),
                                     .requested_lease_ns    = ns_of(k_lease)});
        n.drive();
        REQUIRE(n.a.is_connected(n.id_b));

        const plexus::node_id b_inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_to_a = n.b.session_for(b_inbound);
        REQUIRE(b_to_a != nullptr);

        // One data frame stamps the deadline; then ONLY heartbeats flow (the tick emits
        // them, B publishes no more data).
        n.b.messages().publish("topic", as_bytes("d0"), b_to_a->session_id());
        n.drive();

        // Advance past the deadline (but under the lease): the data lapsed, but the
        // keepalive heartbeats kept presence.
        n.advance(k_period + k_tick_granularity);

        REQUIRE(n.count(liveness_kind::missed_deadline) == 1); // data lapsed
        REQUIRE(n.count(liveness_kind::lease_expired) == 0);   // heartbeat kept presence

        ++proven;
    }
    REQUIRE(proven == k_loops);
}
