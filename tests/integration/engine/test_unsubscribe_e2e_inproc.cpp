#include "recording_observer.h"
#include "test_spine_lifecycle_inproc_common.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace {

using namespace spine_fixture;

using handle = std::optional<plexus::subscriber<>>;

constexpr std::string_view k_topic = "unsub/topic";
constexpr std::array<std::byte, 1> k_payload{std::byte{0x5A}};

// The publisher's observer sees one delivered edge per destination it sends to, so with b as the only
// subscriber its delivered count is the publisher's send count toward b.
struct unsubscribe_pair
{
    pair_fixture fx;
    recording_observer at_a;
    recording_observer at_b;
    plexus::publisher<> pub;

    unsubscribe_pair()
            : pub(fx.a, k_topic)
    {
        fx.a.router().add_observer(at_a);
        fx.b.router().add_observer(at_b);
        fx.connect();
    }

    std::int32_t sent() const
    {
        return at_a.for_topic(k_topic).delivered;
    }

    std::int32_t unsubscribes() const
    {
        return at_b.unsubscribed_for(k_topic);
    }

    bool publisher_holds_b()
    {
        return fx.a.router().messages().has_remote_demand(plexus::wire::fqn_topic_hash(k_topic), nullptr);
    }

    void subscribe(handle &h, std::int32_t &received)
    {
        h.emplace(fx.b, k_topic, [&received](std::span<const std::byte>, const plexus::io::message_info &) { ++received; });
        fx.drive();
    }

    void publish(std::int32_t times)
    {
        for(std::int32_t i = 0; i < times; ++i)
            pub.publish(k_payload);
        fx.drive();
    }
};

void hold_two_and_refan(unsubscribe_pair &p, handle &first, handle &second, std::int32_t &received)
{
    p.subscribe(first, received);
    p.subscribe(second, received);
    p.fx.b.router().subscribe(p.fx.id_a, k_topic);
    p.fx.drive();
    REQUIRE(p.publisher_holds_b());
}

}

TEST_CASE("unsubscribe: two local subscribers and a peer-ready re-fan end in exactly one wire unsubscribe", "[integration][unsubscribe]")
{
    unsubscribe_pair p;
    handle first;
    handle second;
    std::int32_t received = 0;
    hold_two_and_refan(p, first, second, received);

    first.reset();
    p.fx.drive();
    CHECK(p.unsubscribes() == 0);

    second.reset();
    p.fx.drive();
    CHECK(p.unsubscribes() == 1);
}

TEST_CASE("unsubscribe: the publisher stops sending to that peer once the unsubscribe arrives", "[integration][unsubscribe]")
{
    unsubscribe_pair p;
    handle first;
    handle second;
    std::int32_t received = 0;
    hold_two_and_refan(p, first, second, received);
    p.publish(1);
    REQUIRE(p.sent() == 1);
    REQUIRE(received == 2);

    first.reset();
    second.reset();
    p.fx.drive();
    CHECK_FALSE(p.publisher_holds_b());

    p.publish(10);
    CHECK(p.sent() == 1);
    CHECK(received == 2);
}

TEST_CASE("unsubscribe: an unsubscribe for a topic never subscribed emits nothing", "[integration][unsubscribe]")
{
    unsubscribe_pair p;
    handle sub;
    std::int32_t received = 0;
    p.subscribe(sub, received);

    p.fx.b.router().unsubscribe(p.fx.id_a, "unsub/never");
    p.fx.drive();
    CHECK(p.at_b.unsubscribed_for("unsub/never") == 0);
    CHECK(p.unsubscribes() == 0);
    CHECK(p.publisher_holds_b());
}

TEST_CASE("unsubscribe: a resubscribe that follows the unsubscribe on the wire leaves the publisher sending", "[integration][unsubscribe]")
{
    unsubscribe_pair p;
    handle sub;
    std::int32_t dropped_received = 0;
    std::int32_t received         = 0;
    p.subscribe(sub, dropped_received);
    REQUIRE(p.publisher_holds_b());

    sub.reset();
    sub.emplace(p.fx.b, k_topic, [&received](std::span<const std::byte>, const plexus::io::message_info &) { ++received; });
    p.fx.drive();
    CHECK(p.unsubscribes() == 1);
    CHECK(p.publisher_holds_b());

    p.publish(1);
    CHECK(p.sent() == 1);
    CHECK(received == 1);
    CHECK(dropped_received == 0);
}
