#include "support/coordinator_lanes.h"

#include "plexus/io/companion_event.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace {

using plexus::io::demand_role;
using plexus::io::fallback_reason;
using plexus::io::companion_event;
using plexus::io::demand_transition;
using plexus::testing::lane_end;

struct sent_event
{
    std::string node_name;
    std::string fqn;
    companion_event event;

    bool operator==(const sent_event &) const = default;
};

void record_sends(lane_end &end, std::vector<sent_event> &sent)
{
    end.c.on_companion_send([&sent](std::string_view node_name, std::string_view fqn, companion_event event, std::uint32_t)
                            { sent.push_back({std::string{node_name}, std::string{fqn}, event}); });
}

std::vector<sent_event> offered_to(std::vector<std::string> peers)
{
    std::vector<sent_event> sent;
    for(std::string &peer : peers)
        sent.push_back({std::move(peer), "alpha", companion_event::offered});
    return sent;
}

std::uint64_t not_offered(const lane_end &end)
{
    return end.c.fallbacks()[static_cast<std::size_t>(fallback_reason::not_offered)];
}

void await_offer(lane_end &subscriber, std::string_view publisher)
{
    subscriber.c.on_edge(publisher, "alpha", demand_transition::up, demand_role::subscriber);
}

}

TEST_CASE("medium_coordinator_offers a lane a re-attempt mints after the subscribe response is offered to the waiting subscriber", "[shm][medium_coordinator_offers]")
{
    lane_end publisher{1};
    std::vector<sent_event> sent;
    record_sends(publisher, sent);
    publisher.offer("peer");
    REQUIRE(publisher.c.offer_for("peer", "alpha") == companion_event::not_offered);

    publisher.c.on_retry_due();

    REQUIRE(publisher.c.offer_for("peer", "alpha") == companion_event::offered);
    REQUIRE(sent == offered_to({"peer"}));
}

TEST_CASE("medium_coordinator_offers a local publisher declared after the subscribe offers each remembered peer once", "[shm][medium_coordinator_offers]")
{
    lane_end publisher{0, false};
    std::vector<sent_event> sent;
    record_sends(publisher, sent);
    for(const std::string_view peer : {"a", "b", "gone", "dead"})
        publisher.offer(peer);
    publisher.c.on_edge("gone", "alpha", demand_transition::down, demand_role::publisher);
    publisher.c.on_peer_dead("dead");
    REQUIRE(publisher.c.offer_for("a", "alpha") == companion_event::no_publisher);
    REQUIRE(publisher.mints == 0);

    publisher.c.on_local_publisher("alpha", true);

    REQUIRE(publisher.mints == 1);
    REQUIRE(sent == offered_to({"a", "b"}));
    publisher.offer("c");
    REQUIRE(sent == offered_to({"a", "b"}));
    REQUIRE(publisher.c.offer_for("c", "alpha") == companion_event::offered);
}

TEST_CASE("medium_coordinator_offers a transient mint at the publisher's arrival is offered by the mint that serves it", "[shm][medium_coordinator_offers]")
{
    lane_end publisher{1, false};
    std::vector<sent_event> sent;
    record_sends(publisher, sent);
    publisher.offer("a");
    publisher.offer("b");

    publisher.c.on_local_publisher("alpha", true);
    publisher.c.on_retry_due();

    REQUIRE(publisher.mints == 2);
    REQUIRE(sent == offered_to({"a", "b"}));
}

TEST_CASE("medium_coordinator_offers a retired publisher keeps its held lane, re-attempts nothing and answers no publisher", "[shm][medium_coordinator_offers]")
{
    lane_end held;
    held.offer("a");
    held.c.on_companion("a", "alpha", companion_event::ready);
    held.c.on_local_publisher("alpha", false);
    held.offer("c");
    REQUIRE(held.live == 1);
    REQUIRE(held.c.companion_for("a", "alpha", 1).fits);
    REQUIRE(held.c.offer_for("c", "alpha") == companion_event::no_publisher);

    lane_end pending{1};
    pending.offer("b");
    pending.c.on_local_publisher("alpha", false);
    pending.c.on_retry_due();
    REQUIRE(pending.mints == 1);
    REQUIRE(pending.live == 0);
}

TEST_CASE("medium_coordinator_offers a subscriber counts not_offered only for a peer that withheld its lane", "[shm][medium_coordinator_offers]")
{
    lane_end subscriber;
    for(const std::string_view peer : {"withheld", "unpublished"})
        await_offer(subscriber, peer);
    subscriber.c.on_companion("withheld", "alpha", companion_event::not_offered);
    subscriber.c.on_companion("unpublished", "alpha", companion_event::no_publisher);
    REQUIRE(not_offered(subscriber) == 1);

    lane_end declining;
    declining.allow = false;
    await_offer(declining, "withheld");
    declining.c.on_companion("withheld", "alpha", companion_event::not_offered);
    REQUIRE(not_offered(declining) == 0);
}

TEST_CASE("medium_coordinator_offers a response from a peer with no publisher counts nothing and still accepts a late offer", "[shm][medium_coordinator_offers]")
{
    lane_end subscriber;
    std::vector<sent_event> sent;
    record_sends(subscriber, sent);
    await_offer(subscriber, "pub");
    subscriber.c.on_companion("pub", "alpha", companion_event::no_publisher);
    REQUIRE(subscriber.receives == 0);

    subscriber.c.on_companion("pub", "alpha", companion_event::offered);
    subscriber.c.on_companion("pub", "alpha", companion_event::offered);

    REQUIRE(subscriber.receives == 1);
    REQUIRE(not_offered(subscriber) == 0);
    REQUIRE(sent == std::vector<sent_event>{{"pub", "alpha", companion_event::ready}});
}

TEST_CASE("medium_coordinator_offers a late offer rides its own verb and the response states map one to one", "[shm][medium_coordinator_offers]")
{
    using plexus::wire::offer_state;
    REQUIRE(plexus::io::verb_of(companion_event::offered) == plexus::wire::msg_type::companion_offer);
    REQUIRE_FALSE(plexus::io::verb_of(companion_event::not_offered).has_value());
    REQUIRE_FALSE(plexus::io::verb_of(companion_event::no_publisher).has_value());
    for(const offer_state state : {offer_state::no_publisher, offer_state::offered, offer_state::withheld})
        REQUIRE(plexus::io::offer_state_of(plexus::io::offer_event_of(state)) == state);
}
