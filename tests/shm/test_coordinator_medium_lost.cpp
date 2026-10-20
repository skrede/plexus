#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include <catch2/catch_test_macros.hpp>

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
using plexus::io::upgrade_receive;
using plexus::io::companion_event;
using plexus::io::demand_transition;
using plexus::testing::counted_lane;
using plexus::testing::engage_demand;
using plexus::testing::co_host_registry;

using coordinator = plexus::io::upgrade_coordinator<co_host_registry, counted_lane>;

struct lanes
{
    std::int32_t sends    = 0;
    std::int32_t receives = 0;
    std::uint32_t mints   = 0;
};

void install_gates(coordinator &c, lanes &live)
{
    c.on_gate(
            [&live](std::string_view)
            {
                ++live.mints;
                return upgrade_mint<counted_lane>{std::make_unique<counted_lane>(live.sends), [](std::size_t) { return true; }};
            });
    c.on_receive_gate([&live](std::string_view, std::string_view) { return upgrade_receive{[lane = std::make_unique<counted_lane>(live.receives)](std::uint64_t) { (void)lane; }}; });
    c.set_topic_hint("alpha", dispatch_hint::frequent);
    c.set_topic_hint("beta", dispatch_hint::frequent);
    c.on_local_publisher("alpha", true);
    c.on_local_publisher("beta", true);
}

}

TEST_CASE("coordinator_loss a lost medium drops every peer's send and receive lanes for its fqn only and withdraws each receive lane", "[shm][coordinator_loss]")
{
    co_host_registry reg;
    coordinator c{reg};
    lanes live;
    install_gates(c, live);
    std::vector<std::string> withdrawn;
    c.on_companion_send([&withdrawn](std::string_view node_name, std::string_view fqn, companion_event event, std::uint32_t)
                        { withdrawn.push_back(std::string{node_name} + "/" + std::string{fqn} + (event == companion_event::withdrawn ? " withdrawn" : " ready")); });
    engage_demand(c, "node-a", "alpha", demand_role::publisher);
    engage_demand(c, "node-b", "alpha", demand_role::subscriber);
    engage_demand(c, "node-a", "beta", demand_role::publisher);
    REQUIRE(live.sends == 2);
    REQUIRE(live.receives == 1);

    c.on_medium_lost("alpha");

    CHECK(live.sends == 1);
    CHECK(live.receives == 0);
    CHECK(!c.companion_for("node-a", "alpha", 1).fits);
    CHECK(c.companion_for("node-a", "beta", 1).fits);
    CHECK(withdrawn == std::vector<std::string>{"node-b/alpha ready", "node-b/alpha withdrawn"});
}

TEST_CASE("coordinator_loss the next up-edge after a lost medium mints afresh", "[shm][coordinator_loss]")
{
    co_host_registry reg;
    coordinator c{reg};
    lanes live;
    install_gates(c, live);
    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    c.on_medium_lost("alpha");
    REQUIRE(live.sends == 0);

    engage_demand(c, "node-a", "alpha", demand_role::publisher);

    CHECK(live.mints == 2);
    CHECK(live.sends == 1);
    CHECK(c.companion_for("node-a", "alpha", 1).fits);
}
