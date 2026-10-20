#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

using plexus::io::demand_role;
using plexus::io::dispatch_hint;
using plexus::io::upgrade_mint;
using plexus::io::fallback_reason;
using plexus::io::upgrade_receive;
using plexus::io::companion_event;
using plexus::io::demand_transition;
using plexus::testing::counted_lane;
using plexus::testing::co_host_registry;

using coordinator = plexus::io::upgrade_coordinator<co_host_registry, counted_lane>;

// One end of a co-host pair: its lanes, what it answered, and a receive gate that declines its first declines attaches.
struct pair_end
{
    explicit pair_end(bool hinted, std::uint32_t declines = 0)
            : sends(0)
            , receives(0)
            , wakes(0)
            , declines(declines)
            , c(registry)
    {
        c.on_gate([this](std::string_view) { return upgrade_mint<counted_lane>{std::make_unique<counted_lane>(sends), [](std::size_t) { return true; }}; });
        c.on_receive_gate([this](std::string_view, std::string_view) { return receive(); });
        c.on_companion_send([this](std::string_view, std::string_view, companion_event event, std::uint32_t) { answers.push_back(event); });
        c.set_retry_scheduler([this](std::chrono::milliseconds) { ++wakes; });
        c.set_topic_hint("alpha", hinted ? dispatch_hint::frequent : dispatch_hint::none);
        c.on_local_publisher("alpha", true);
    }

    upgrade_receive receive()
    {
        if(declines == 0)
            return upgrade_receive{[lane = std::make_unique<counted_lane>(receives)](std::uint64_t) { (void)lane; }};
        --declines;
        return upgrade_receive{fallback_reason::transient};
    }

    std::uint64_t counted(fallback_reason reason) const
    {
        return c.fallbacks()[static_cast<std::size_t>(reason)];
    }

    std::int32_t sends;
    std::int32_t receives;
    std::uint32_t wakes;
    std::uint32_t declines;
    co_host_registry registry;
    coordinator c;
    std::vector<companion_event> answers;
};

// The subscribe reaches the publisher, its response carries the offer back, and the subscriber's answers reach it.
void subscribe(pair_end &publisher, pair_end &subscriber)
{
    subscriber.c.on_edge("pub", "alpha", demand_transition::up, demand_role::subscriber);
    publisher.c.on_edge("sub", "alpha", demand_transition::up, demand_role::publisher);
    subscriber.c.on_companion("pub", "alpha", publisher.c.offer_for("sub", "alpha"));
    for(const companion_event answer : subscriber.answers)
        publisher.c.on_companion("sub", "alpha", answer);
}

}

TEST_CASE("medium_coordinator_engagement an offered ring both policies allow attaches and is answered ready", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    pair_end subscriber{true};
    subscriber.c.on_edge("pub", "alpha", demand_transition::up, demand_role::subscriber);
    REQUIRE(subscriber.receives == 0);

    subscribe(publisher, subscriber);

    REQUIRE(subscriber.receives == 1);
    REQUIRE(subscriber.answers == std::vector<companion_event>{companion_event::ready});
    REQUIRE(publisher.c.companion_for("sub", "alpha", 1).fits);
}

TEST_CASE("medium_coordinator_engagement a publisher routes over its lane only after the ready answer", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    publisher.c.on_edge("sub", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(publisher.c.offer_for("sub", "alpha") == companion_event::offered);
    REQUIRE(!publisher.c.companion_for("sub", "alpha", 1).fits);

    publisher.c.on_companion("sub", "alpha", companion_event::ready);
    REQUIRE(publisher.c.companion_for("sub", "alpha", 1).fits);
}

TEST_CASE("medium_coordinator_engagement an unoffered response attaches nothing and counts not_offered only when wanted", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{false};
    pair_end subscriber{true};
    pair_end unhinted{false};
    subscribe(publisher, subscriber);
    subscribe(publisher, unhinted);

    REQUIRE(subscriber.receives == 0);
    REQUIRE(subscriber.answers.empty());
    REQUIRE(subscriber.counted(fallback_reason::not_offered) == 1);
    REQUIRE(unhinted.counted(fallback_reason::not_offered) == 0);
}

TEST_CASE("medium_coordinator_engagement a hinted publisher and an unhinted subscriber keep the wire", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    pair_end subscriber{false};
    subscribe(publisher, subscriber);

    REQUIRE(publisher.c.offer_for("sub", "alpha") == companion_event::offered);
    REQUIRE(subscriber.receives == 0);
    REQUIRE(!publisher.c.companion_for("sub", "alpha", 1).fits);
}

TEST_CASE("medium_coordinator_engagement a lost receive lane is withdrawn and the publisher returns to the wire", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    pair_end subscriber{true};
    subscribe(publisher, subscriber);

    subscriber.c.on_medium_lost("alpha");
    publisher.c.on_companion("sub", "alpha", subscriber.answers.back());

    REQUIRE(subscriber.answers == std::vector<companion_event>{companion_event::ready, companion_event::withdrawn});
    REQUIRE(subscriber.receives == 0);
    REQUIRE(subscriber.counted(fallback_reason::withdrawn) == 1);
    REQUIRE(publisher.sends == 0);
    REQUIRE(publisher.c.offer_for("sub", "alpha") == companion_event::not_offered);
    REQUIRE(!publisher.c.companion_for("sub", "alpha", 1).fits);
    REQUIRE(publisher.counted(fallback_reason::withdrawn) == 1);
}

TEST_CASE("medium_coordinator_engagement a clean unsubscribe drops both lanes and withdraws nothing", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    pair_end subscriber{true};
    subscribe(publisher, subscriber);

    subscriber.c.on_edge("pub", "alpha", demand_transition::down, demand_role::subscriber);
    publisher.c.on_edge("sub", "alpha", demand_transition::down, demand_role::publisher);

    REQUIRE(subscriber.answers == std::vector<companion_event>{companion_event::ready});
    REQUIRE(subscriber.receives == 0);
    REQUIRE(publisher.sends == 0);
    REQUIRE(subscriber.counted(fallback_reason::withdrawn) == 0);
    subscriber.c.on_companion("pub", "alpha", companion_event::offered);
    REQUIRE(subscriber.receives == 0);
}

TEST_CASE("medium_coordinator_engagement peer death clears the offered and ready lanes", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    pair_end subscriber{true};
    subscribe(publisher, subscriber);

    publisher.c.on_peer_dead("sub");
    subscriber.c.on_peer_dead("pub");
    publisher.c.on_companion("sub", "alpha", companion_event::ready);
    subscriber.c.on_companion("pub", "alpha", companion_event::offered);

    REQUIRE(publisher.c.offer_for("sub", "alpha") == companion_event::not_offered);
    REQUIRE(!publisher.c.companion_for("sub", "alpha", 1).fits);
    REQUIRE(subscriber.receives == 0);
}

TEST_CASE("medium_coordinator_engagement a transient attach on the offer is re-attempted and answered ready once", "[shm][medium_coordinator_engagement]")
{
    pair_end publisher{true};
    pair_end subscriber{true, 2};
    subscribe(publisher, subscriber);
    REQUIRE(subscriber.answers.empty());

    subscriber.c.on_retry_due();
    subscriber.c.on_retry_due();

    REQUIRE(subscriber.wakes == 2);
    REQUIRE(subscriber.receives == 1);
    REQUIRE(subscriber.counted(fallback_reason::transient) == 2);
    REQUIRE(subscriber.answers == std::vector<companion_event>{companion_event::ready});
}
