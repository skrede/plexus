#include "test_rxo_compatibility_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace rxo_fixture;

TEST_CASE("rxo compatibility: a strict incompatible reliability pair is refused, a compatible pair "
          "communicates")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // The incompatible leg: the producer offers best_effort, the strict subscriber
        // requests reliable -> refused with incompatible_qos, NO data delivered.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.reliability = reliability::best_effort});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_reliability_reliable = true, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
            REQUIRE(l.degraded.empty());

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.empty()); // refused: no fan-out entry, no data
        }
        // The compatible leg: the producer offers reliable, the same strict subscriber
        // is admitted and a publish DELIVERS, with no refusal/degraded fire.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.reliability = reliability::reliable});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_reliability_reliable = true, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.size() == 1);
            REQUIRE(l.received[0] == "payload");
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: the same incompatible reliability pair connects under permissive and "
          "surfaces the degraded field")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        link l;
        l.drive();
        REQUIRE(l.complete());
        l.prod_messages.declare("topic", plexus::topic_qos{.reliability = reliability::best_effort});
        // The SAME best_effort/reliable mismatch under permissive: connect + surface.
        l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_reliability_reliable = true, .rxo = rxo_mode::permissive});
        l.drive();
        REQUIRE(l.refusals.empty());
        REQUIRE(l.degraded.size() == 1);                         // the observable FIRED
        REQUIRE(l.degraded[0] != 0);                             // NON-EMPTY (non-silent)
        REQUIRE((l.degraded[0] & k_rxo_field_reliability) != 0); // names the right field

        l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
        l.drive();
        REQUIRE(l.received.size() == 1); // permissive: data flows
        REQUIRE(l.received[0] == "payload");
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a strict incompatible durability pair is refused and permissive "
          "surfaces it")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // A non-latching producer offers `none`; a durability::all request is incompatible.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.latch = false});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::all, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
        }
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.latch = false});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::all, .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.size() == 1);
            REQUIRE((l.degraded[0] & k_rxo_field_durability) != 0);
        }
        // A latching producer + the same request is admitted with no degraded fire.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.latch = true});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::all, .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}
