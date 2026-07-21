// A directly-attached publisher's PLAIN publish transits publisher -> relay -> remote subscriber end to
// end, with NO direct publisher<->subscriber session: the relay re-originates the publish as a forwarded
// envelope stamping the publisher's handshake-proven identity, gated by the reused forward-scope matcher
// and a self-route-aware non-local-demand predicate, and delivers it to the subscriber with the origin as
// source and the bytes intact. An accepted publisher session (the publisher dials the relay) still stamps
// the publisher's proven gid; an out-of-scope topic and a purely-local-demand topic originate nothing;
// loop-safety and dedup hold.

#include "test_forward_pubsub_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace forward_pubsub_fixture;

TEST_CASE("forward_pubsub_transit: a directly-attached publisher's plain publish transits to a remote subscriber "
          "through a relay, stamping the publisher origin",
          "[integration][forward_pubsub]")
{
    manual_clock::reset();
    pubsub_line line;
    line.connect("telemetry/temp", /*emit_source_identity=*/true, /*publisher_dials=*/false);
    line.subscribe_through_relay("telemetry/temp");

    REQUIRE(line.no_direct_pub_sub_session()); // the whole path rides the relay

    line.publish("telemetry/temp", "24.5C");

    REQUIRE(line.sub_got.size() == 1u);                       // reached the one demanded subscriber, exactly once
    REQUIRE(line.sub_got.front().fqn == "telemetry/temp");
    REQUIRE(line.sub_got.front().body == "24.5C");            // delivered bytes == published bytes
    REQUIRE(line.sub_got.front().info.source_identity.has_value());
    REQUIRE(line.sub_got.front().info.source_identity->node_id() == line.id_pub); // origin is the publisher, not the relay
    REQUIRE(line.pub_got.empty());                            // the publisher never receives its own frame back (arrival-excluded)
}

TEST_CASE("forward_pubsub_transit: an accepted publisher session (the publisher dials the relay) still stamps the "
          "publisher's handshake-proven identity, not the provisional slot key",
          "[integration][forward_pubsub][accepted]")
{
    manual_clock::reset();
    pubsub_line line;
    line.connect("telemetry/accel", /*emit_source_identity=*/true, /*publisher_dials=*/true);
    line.subscribe_through_relay("telemetry/accel");
    REQUIRE(line.no_direct_pub_sub_session());

    line.publish("telemetry/accel", "9.8");

    REQUIRE(line.sub_got.size() == 1u);
    REQUIRE(line.sub_got.front().info.source_identity.has_value());
    REQUIRE(line.sub_got.front().info.source_identity->node_id() == line.id_pub); // proven gid on the accepted session
}

TEST_CASE("forward_pubsub_transit: a source-identity publish round-trips the endpoint counter through the "
          "re-origination (header flags survive)",
          "[integration][forward_pubsub][source-identity]")
{
    manual_clock::reset();
    pubsub_line line;
    line.connect("telemetry/rpm", /*emit_source_identity=*/true, /*publisher_dials=*/false);
    line.subscribe_through_relay("telemetry/rpm");

    line.publish("telemetry/rpm", "3000");

    REQUIRE(line.sub_got.size() == 1u);
    REQUIRE(line.sub_got.front().info.source_identity.has_value());
    REQUIRE(line.sub_got.front().info.source_identity->endpoint_counter() != 0u); // the counter survived decode_unidirectional
}

TEST_CASE("forward_pubsub_transit: an out-of-scope topic originates nothing while an in-scope topic transits",
          "[integration][forward_pubsub][scope]")
{
    manual_clock::reset();
    plexus::io::forward_options scoped;
    scoped.scope_pattern = "sensor/*"; // admits sensor/*, excludes cmd/*
    pubsub_line line{scoped};
    line.connect("sensor/temp", /*emit_source_identity=*/false, /*publisher_dials=*/false);
    line.connect("cmd/reset", /*emit_source_identity=*/false, /*publisher_dials=*/false);
    line.subscribe_through_relay("sensor/temp");
    line.subscribe_through_relay("cmd/reset");

    line.publish("sensor/temp", "in-scope");
    line.publish("cmd/reset", "out-of-scope");

    REQUIRE(line.sub_got.size() == 1u); // only the in-scope topic produced forwarded egress
    REQUIRE(line.sub_got.front().fqn == "sensor/temp");
    REQUIRE(line.sub_got.front().body == "in-scope");
}

TEST_CASE("forward_pubsub_transit: two distinct publishes each deliver once downstream (distinct per-relay seq, "
          "no accidental dedup)",
          "[integration][forward_pubsub][dedup]")
{
    manual_clock::reset();
    pubsub_line line;
    line.connect("telemetry/seq", /*emit_source_identity=*/false, /*publisher_dials=*/false);
    line.subscribe_through_relay("telemetry/seq");

    line.publish("telemetry/seq", "first");
    line.publish("telemetry/seq", "second");

    REQUIRE(line.sub_got.size() == 2u);
    REQUIRE(line.sub_got[0].body == "first");
    REQUIRE(line.sub_got[1].body == "second");
}

TEST_CASE("forward_pubsub_transit: a purely-locally-demanded topic (only a self-route subscriber) originates "
          "nothing — no forwarded envelope onto the self-channel",
          "[integration][forward_pubsub][purely-local]")
{
    manual_clock::reset();
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t tr{ex, bus};
    plexus::log::null_logger sink;
    relay_engine relay(tr, ex, make_cfg(0x2A), k_long_timeout, forever_cfg(), k_seed, sink);

    pubsub_channel self_sink{ex};
    pubsub_channel self_route{ex};
    self_route.connect_to(self_sink.local_endpoint());
    std::vector<std::vector<std::byte>> self_frames;
    self_sink.on_data([&](std::span<const std::byte> f) { self_frames.emplace_back(f.begin(), f.end()); });

    relay.messages().attach_local("local/only", self_route, "relay-self");
    ex.drain();

    const auto inner = capture_inner("local/only", "body");
    relay.messages().originate_forwarded(wire::fqn_topic_hash("local/only"), make_id(0xB0), inner, nullptr);
    ex.drain();

    REQUIRE(count_forwarded(self_frames) == 0); // the self-route is excluded from non-local demand: no 0x0F onto the self-channel
}

TEST_CASE("forward_pubsub_transit: origination excludes the arrival session (loop guard) and fires only for a "
          "genuinely remote subscriber",
          "[integration][forward_pubsub][loop-safety]")
{
    manual_clock::reset();
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t tr{ex, bus};
    plexus::log::null_logger sink;
    relay_engine relay(tr, ex, make_cfg(0x2A), k_long_timeout, forever_cfg(), k_seed, sink);

    pubsub_channel rem_sink{ex};
    pubsub_channel rem{ex};
    rem.connect_to(rem_sink.local_endpoint());
    std::vector<std::vector<std::byte>> rem_frames;
    rem_sink.on_data([&](std::span<const std::byte> f) { rem_frames.emplace_back(f.begin(), f.end()); });

    relay.messages().attach_for_fanout(pubsub_forwarder::peer{rem, "downstream"}, "remote/topic");
    ex.drain();

    const auto inner      = capture_inner("remote/topic", "body");
    const std::uint64_t h = wire::fqn_topic_hash("remote/topic");

    // The sole subscriber IS the arrival session: excluded, so a publisher never gets its own frame back.
    relay.messages().originate_forwarded(h, make_id(0xB0), inner, &rem);
    ex.drain();
    REQUIRE(count_forwarded(rem_frames) == 0);

    // A genuinely remote arrival (nullptr here) leaves the subscriber as non-local demand: it originates.
    relay.messages().originate_forwarded(h, make_id(0xB0), inner, nullptr);
    ex.drain();
    REQUIRE(count_forwarded(rem_frames) == 1);
}
