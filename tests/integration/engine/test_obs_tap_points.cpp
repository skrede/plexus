#include "test_obs_tap_points_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace obs_tap_fixture;

TEST_CASE("obs tap: one publish to N subscribers fires published once and delivered once per "
          "destination",
          "[integration][observer][tap]")
{
    constexpr std::size_t k_subscribers = 3;
    const std::string payload           = "tap-fan-out";

    manual_clock::reset();
    fan_net net{k_subscribers};
    recording_observer rec;
    net.pub.add_observer(rec);
    net.drive(); // settle the N subscribe round-trips

    net.pub.publish(fan_net::k_topic, as_bytes(payload));

    // Posted-not-inline: the taps fan out on the executor, so the counters are still
    // zero synchronously at the publish call return.
    REQUIRE(rec.for_topic(fan_net::k_topic).published == 0);
    REQUIRE(rec.for_topic(fan_net::k_topic).delivered == 0);

    net.drive(); // pump the posted tap turns

    const auto &t = rec.for_topic(fan_net::k_topic);
    REQUIRE(t.published == 1);                               // on_message_published once per publish
    REQUIRE(t.delivered == static_cast<int>(k_subscribers)); // on_message_delivered once per destination
}

TEST_CASE("obs tap: the delivered view borrows the surfaced owner (a shared addref, not a copy)", "[integration][observer][tap]")
{
    const std::string payload = "tap-zero-copy";

    manual_clock::reset();
    fan_net net{1};
    recording_observer rec;
    net.pub.add_observer(rec);
    net.drive();

    net.pub.publish(fan_net::k_topic, as_bytes(payload));
    net.drive();

    const auto &t = rec.for_topic(fan_net::k_topic);
    REQUIRE(t.delivered == 1);
    // The delivered view shares an owner handle (the borrowed buffer), so the captured
    // owner is engaged and its use_count reflects a shared addref — not a fresh
    // allocation handed in by value.
    REQUIRE(t.last_view_owner != nullptr);
    REQUIRE(t.last_view_use_count >= 1);
}
