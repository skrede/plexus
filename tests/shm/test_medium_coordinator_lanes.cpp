#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

using plexus::io::demand_role;
using plexus::io::dispatch_hint;
using plexus::io::fallback_reason;
using plexus::io::companion_event;
using plexus::io::demand_transition;
using plexus::testing::lane_end;
using plexus::testing::counted_lane;

}

TEST_CASE("medium_coordinator_lanes one lane per topic serves every offered peer", "[shm][medium_coordinator_lanes]")
{
    lane_end p;
    p.offer("a");
    p.offer("b");
    REQUIRE(p.mints == 1);
    REQUIRE(p.c.offer_for("a", "alpha") == companion_event::offered);
    REQUIRE(p.c.offer_for("b", "alpha") == companion_event::offered);
    REQUIRE(p.c.lane_for("alpha") != nullptr);

    p.c.set_topic_hint("beta", dispatch_hint::frequent);
    p.c.on_local_publisher("beta", true);
    p.c.on_edge("a", "beta", demand_transition::up, demand_role::publisher);
    REQUIRE(p.mints == 2);
    REQUIRE(p.live == 2);
}

TEST_CASE("medium_coordinator_lanes an offered peer routes over the lane only once it answered ready", "[shm][medium_coordinator_lanes]")
{
    lane_end p;
    p.offer("a");
    p.offer("b");
    REQUIRE(!p.c.companion_for("a", "alpha", 1).fits);

    p.c.on_companion("a", "alpha", companion_event::ready);
    REQUIRE(p.c.companion_for("a", "alpha", 1).lane == p.c.lane_for("alpha"));
    REQUIRE(!p.c.companion_for("b", "alpha", 1).fits);
}

TEST_CASE("medium_coordinator_lanes the lane outlives one holder's withdrawal while another holder remains", "[shm][medium_coordinator_lanes]")
{
    lane_end p;
    p.offer("a");
    p.offer("b");
    p.c.on_companion("a", "alpha", companion_event::ready);

    p.c.on_companion("b", "alpha", companion_event::withdrawn);

    REQUIRE(p.live == 1);
    REQUIRE(p.released == 0);
    REQUIRE(p.c.offer_for("b", "alpha") == companion_event::not_offered);
    REQUIRE(p.c.companion_for("a", "alpha", 1).fits);
    REQUIRE(p.c.fallbacks()[static_cast<std::size_t>(fallback_reason::withdrawn)] == 1);
}

TEST_CASE("medium_coordinator_lanes the last holder's release fires the hook once, before the channel goes", "[shm][medium_coordinator_lanes]")
{
    const std::uint32_t path = GENERATE(0u, 1u, 2u, 3u);
    lane_end p;
    p.offer("a");
    p.c.on_companion("a", "alpha", companion_event::ready);
    if(path == 3)
        REQUIRE(p.c.hold_self("alpha") != nullptr);

    if(path == 0)
        p.c.on_edge("a", "alpha", demand_transition::down, demand_role::publisher);
    else if(path == 1)
        p.c.on_companion("a", "alpha", companion_event::withdrawn);
    else if(path == 2)
        p.c.on_peer_dead("a");
    else
        p.c.on_medium_lost("alpha");

    REQUIRE(p.released == 1);
    REQUIRE(p.live_at_release == 1);
    REQUIRE(p.live == 0);
    REQUIRE(p.c.lane_for("alpha") == nullptr);
    REQUIRE(p.c.self_channel("alpha") == nullptr);
}

TEST_CASE("medium_coordinator_lanes receive lanes are kept per publisher", "[shm][medium_coordinator_lanes]")
{
    lane_end s;
    plexus::testing::engage_demand(s.c, "pub-a", "alpha", demand_role::subscriber);
    plexus::testing::engage_demand(s.c, "pub-b", "alpha", demand_role::subscriber);
    REQUIRE(s.receives == 2);

    s.c.on_edge("pub-a", "alpha", demand_transition::down, demand_role::subscriber);
    REQUIRE(s.receives == 1);
    s.c.on_peer_dead("pub-b");
    REQUIRE(s.receives == 0);
    REQUIRE(s.mints == 0);
}

TEST_CASE("medium_coordinator_lanes the node's own hold mints whatever the policy and a peer joins only when it allows", "[shm][medium_coordinator_lanes]")
{
    lane_end p;
    p.allow           = false;
    counted_lane *own = p.c.hold_self("alpha");
    REQUIRE(own != nullptr);
    REQUIRE(p.c.self_channel("alpha") == own);

    p.offer("a");
    REQUIRE(p.c.offer_for("a", "alpha") == companion_event::not_offered);
    p.allow = true;
    p.offer("b");
    REQUIRE(p.c.offer_for("b", "alpha") == companion_event::offered);
    REQUIRE(p.mints == 1);
}

TEST_CASE("medium_coordinator_lanes the lane survives the node's own release while a ready peer holds it, and the reverse", "[shm][medium_coordinator_lanes]")
{
    const bool self_first = GENERATE(true, false);
    lane_end p;
    p.offer("a");
    p.c.on_companion("a", "alpha", companion_event::ready);
    REQUIRE(p.c.hold_self("alpha") == p.c.lane_for("alpha"));

    if(self_first)
        p.c.release_self("alpha");
    else
        p.c.on_edge("a", "alpha", demand_transition::down, demand_role::publisher);

    REQUIRE(p.live == 1);
    REQUIRE(p.released == 0);
    REQUIRE((p.c.self_channel("alpha") == nullptr) == self_first);
    REQUIRE((!p.c.companion_for("a", "alpha", 1).fits) != self_first);
}

TEST_CASE("medium_coordinator_lanes a lane minted on a re-attempt serves every peer still waiting", "[shm][medium_coordinator_lanes]")
{
    lane_end p{2};
    p.offer("a");
    p.offer("b");
    REQUIRE(p.c.offer_for("a", "alpha") == companion_event::not_offered);
    REQUIRE(p.c.offer_for("b", "alpha") == companion_event::not_offered);

    p.c.on_retry_due();
    p.c.on_retry_due();

    REQUIRE(p.mints == 3);
    REQUIRE(p.live == 1);
    REQUIRE(p.c.offer_for("a", "alpha") == companion_event::offered);
    REQUIRE(p.c.offer_for("b", "alpha") == companion_event::offered);
}

TEST_CASE("medium_coordinator_lanes a declined self hold holds nothing and counts its reason", "[shm][medium_coordinator_lanes]")
{
    lane_end p{1};
    REQUIRE(p.c.hold_self("alpha") == nullptr);
    REQUIRE(p.c.fallbacks()[static_cast<std::size_t>(fallback_reason::transient)] == 1);
    p.c.release_self("alpha");
    REQUIRE(p.released == 0);
}
