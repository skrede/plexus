// The subscriber-QoS wire round-trip identity unit: from_wire_region(to_wire_region(q))
// reproduces q over every requested_flags bit (each set independently), a couple of
// combinations, and the all-clear default. The two free functions live in
// subscribe_qos_wire.h. wants_message_info is the one
// subscriber_qos field that NEVER crosses the wire (an inproc-local stamp demand), so it
// stays at its default in every swept value — the region cannot carry it back.

#include "plexus/io/subscribe_qos_wire.h"
#include "plexus/io/subscriber_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using plexus::io::durability;
using plexus::io::delivery;
using plexus::io::rxo_mode;
using plexus::io::attach_posture;
using plexus::io::subscriber_qos;
using plexus::io::to_wire_region;
using plexus::io::from_wire_region;

namespace {

bool round_trips(const subscriber_qos &q)
{
    return from_wire_region(to_wire_region(q)) == q;
}

}

TEST_CASE("subscribe_qos_wire: the all-clear default round-trips to itself")
{
    REQUIRE(round_trips(subscriber_qos{}));
}

TEST_CASE("subscribe_qos_wire: each requested_flags bit round-trips when set independently")
{
    REQUIRE(round_trips(subscriber_qos{.requires_source_identity = true}));
    REQUIRE(round_trips(subscriber_qos{.requested_reliability_reliable = true}));
    REQUIRE(round_trips(subscriber_qos{.rxo = rxo_mode::strict}));
    REQUIRE(round_trips(subscriber_qos{.posture = attach_posture::strict}));
}

TEST_CASE("subscribe_qos_wire: the scalar and enum fields round-trip")
{
    REQUIRE(round_trips(subscriber_qos{.durability_mode = durability::all}));
    REQUIRE(round_trips(subscriber_qos{.delivery_mode = delivery::pull}));
    REQUIRE(round_trips(subscriber_qos{.replay_depth = 0xDEADBEEFu}));
    REQUIRE(round_trips(subscriber_qos{.requested_deadline_ns = 1'000'000ull}));
    REQUIRE(round_trips(subscriber_qos{.requested_lease_ns = 9'999'999ull}));
    REQUIRE(round_trips(subscriber_qos{.requested_priority = 7}));
    REQUIRE(round_trips(subscriber_qos{.requested_max_message_bytes = 0x00ABCDEFu}));
    REQUIRE(round_trips(subscriber_qos{.requested_max_message_bytes = 0xFFFFFFFFu}));
}

TEST_CASE("subscribe_qos_wire: combinations of every flag and field round-trip together")
{
    REQUIRE(round_trips(subscriber_qos{.durability_mode                = durability::latest,
                                       .delivery_mode                  = delivery::pull,
                                       .replay_depth                   = 32,
                                       .requires_source_identity       = true,
                                       .requested_reliability_reliable = true,
                                       .requested_deadline_ns          = 200,
                                       .requested_lease_ns             = 400,
                                       .requested_priority             = 3,
                                       .requested_max_message_bytes    = 8u * 1024u * 1024u,
                                       .rxo                            = rxo_mode::strict,
                                       .posture                        = attach_posture::strict}));

    REQUIRE(round_trips(subscriber_qos{.durability_mode = durability::all, .requested_reliability_reliable = true, .rxo = rxo_mode::strict}));
}
