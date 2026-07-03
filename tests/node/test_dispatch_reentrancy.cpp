// Dispatch re-entrancy: a subscription callback may (un)subscribe on the same node while the fan is
// in flight. Delivery is posted onto a clean executor turn — the natural place for user code to
// (un)subscribe. The dispatch must fan from a frozen view and defer the edit: a subscribe that grows
// the subscription table must not relocate it under the callback loop, and an unsubscribe of a live
// entry must retire exactly once with no double-delivery and no iterator invalidation. asan/ubsan is
// the load-bearing gate — a pre-fix live-iteration trips a use-after-free here.

#include "test_self_loopback_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <optional>

using plexus_test::fixture;

TEST_CASE("dispatch_reentrancy: subscribe-from-callback survives reallocation and the new subscriber gets a later publish", "[node][dispatch][reentrancy]")
{
    fixture f;

    // Many subscribers on the source topic, each of which subscribes a new sink-topic subscriber the
    // moment it fires. Under a pre-fix live iteration these repeated registrations relocate the
    // subscription table mid-fan (the sanitizers trip on the dangling loop reference); the fix fans
    // from a frozen view and applies the adds after the loop.
    constexpr int kSources = 64;

    int source_fired   = 0;
    int sink_delivered = 0;
    std::vector<plexus::subscriber<>> sources;
    std::vector<plexus::subscriber<>> sinks;
    sources.reserve(kSources);
    sinks.reserve(kSources);

    for(int i = 0; i < kSources; ++i)
        sources.emplace_back(f.node(), "source",
                             [&](std::span<const std::byte>)
                             {
                                 ++source_fired;
                                 sinks.emplace_back(f.node(), "sink", [&](std::span<const std::byte>) { ++sink_delivered; });
                             });

    plexus::publisher<> source_pub{f.node(), "source"};
    plexus::publisher<> sink_pub{f.node(), "sink"};
    f.drive();

    const std::array<std::byte, 1> payload{std::byte{0x01}};
    source_pub.publish(payload);
    f.drive();

    REQUIRE(source_fired == kSources);
    REQUIRE(sink_delivered == 0); // a subscriber added during the fan does not receive the in-flight publish

    sink_pub.publish(payload);
    f.drive();
    REQUIRE(sink_delivered == kSources); // each subscriber added mid-fan receives the subsequent publish
}

TEST_CASE("dispatch_reentrancy: unsubscribe-self-from-callback retires exactly once with no double-delivery", "[node][dispatch][reentrancy]")
{
    fixture f;

    int fired_self  = 0;
    int fired_other = 0;
    std::optional<plexus::subscriber<>> self_sub;
    self_sub.emplace(f.node(), "topic",
                     [&](std::span<const std::byte>)
                     {
                         ++fired_self;
                         self_sub.reset(); // drop the own handle from inside the callback
                     });
    plexus::subscriber<> other{f.node(), "topic", [&](std::span<const std::byte>) { ++fired_other; }};
    plexus::publisher<> pub{f.node(), "topic"};
    f.drive();

    const std::array<std::byte, 1> payload{std::byte{0x07}};
    pub.publish(payload);
    f.drive();

    // The self-dropping subscriber fired exactly once; the sibling after it in the fan still fired
    // exactly once — the deferred erase left the loop intact (no iterator UB, no skipped delivery).
    REQUIRE(fired_self == 1);
    REQUIRE(fired_other == 1);

    // The retired subscriber is gone; a second publish reaches only the survivor.
    pub.publish(payload);
    f.drive();
    REQUIRE(fired_self == 1);
    REQUIRE(fired_other == 2);
}
