#include "test_rxo_compatibility_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace rxo_fixture;

TEST_CASE("rxo compatibility: a pub max-message-bytes over the sub-requested ceiling refuses under "
          "strict and surfaces under permissive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // The producer declares a 16 MiB per-message max; the subscriber requests a
        // 4 MiB ceiling — the publisher can emit larger than the subscriber accepts, the
        // incompatible direction. Strict refuses with the size bit named.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.max_message_bytes = 16u * 1024u * 1024u});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_max_message_bytes = 4u * 1024u * 1024u, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
            REQUIRE(l.degraded.empty());

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.empty()); // refused: no fan-out entry, no data
        }
        // The SAME pair under permissive connects, data flows, and the size bit surfaces.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.max_message_bytes = 16u * 1024u * 1024u});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_max_message_bytes = 4u * 1024u * 1024u, .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.size() == 1);
            REQUIRE(l.degraded[0] != 0);
            REQUIRE((l.degraded[0] & k_rxo_field_max_message_bytes) != 0);

            l.prod_messages.publish("topic", as_bytes("payload"), l.producer->session_id());
            l.drive();
            REQUIRE(l.received.size() == 1);
            REQUIRE(l.received[0] == "payload");
        }
        // A subscriber requesting a ceiling AT/ABOVE the producer's max is admitted clean.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.max_message_bytes = 4u * 1024u * 1024u});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_max_message_bytes = 16u * 1024u * 1024u, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a requires source identity subscriber is refused against a "
          "non-offering producer regardless of mode")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        for(auto mode : {rxo_mode::permissive, rxo_mode::strict})
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            // The producer declares WITHOUT emit_source_identity (the 4th arg defaults false).
            l.prod_messages.declare("topic", plexus::topic_qos{});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requires_source_identity = true, .rxo = mode});
            l.drive();
            // The always-hard floor: refused regardless of mode, and the degraded
            // observable must NOT fire (it is not a degradable field).
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::source_identity_incompatible);
            REQUIRE(l.degraded.empty());
        }
        // An offering producer admits the same requiring subscriber.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{}, std::nullopt,
                                    /*emit_source_identity=*/true);
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requires_source_identity = true, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("rxo compatibility: a deadline or lease soft mismatch refuses under strict and surfaces "
          "under permissive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        // The producer offers a SLOWER deadline than the subscriber requests
        // (offered 200 > requested 100) — an incompatible offer.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.offered_deadline_ns = 200});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_deadline_ns = 100, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.size() == 1);
            REQUIRE(l.refusals[0] == subscribe_status::incompatible_qos);
        }
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.offered_deadline_ns = 200});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_deadline_ns = 100, .rxo = rxo_mode::permissive});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.size() == 1);
            REQUIRE((l.degraded[0] & k_rxo_field_deadline) != 0);
        }
        // An unset offered deadline (0) is always compatible — no fire in either mode.
        {
            link l;
            l.drive();
            REQUIRE(l.complete());
            l.prod_messages.declare("topic", plexus::topic_qos{.offered_deadline_ns = 0});
            l.subscriber->subscribe("topic", subscriber_qos{.durability_mode = durability::none, .requested_deadline_ns = 100, .rxo = rxo_mode::strict});
            l.drive();
            REQUIRE(l.refusals.empty());
            REQUIRE(l.degraded.empty());
        }
        ++proven;
    }
    REQUIRE(proven == k_loops);
}
