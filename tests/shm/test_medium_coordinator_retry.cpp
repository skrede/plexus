#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"
#include "plexus/io/detail/upgrade_retry.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

using plexus::io::demand_role;
using plexus::io::upgrade_mint;
using plexus::io::fallback_reason;
using plexus::io::upgrade_receive;
using plexus::io::demand_transition;
using std::chrono::milliseconds;
using plexus::testing::counted_lane;
using plexus::testing::engage_demand;
using plexus::testing::co_host_registry;

using coordinator = plexus::io::upgrade_coordinator<co_host_registry, counted_lane>;

// Declines with reason for the first declines calls, then engages.
struct flaky_gate
{
    flaky_gate(std::uint32_t declines, fallback_reason reason)
            : live(0)
            , calls(0)
            , declines(declines)
            , reason(reason)
    {
    }

    std::int32_t live;
    std::uint32_t calls;
    std::uint32_t declines;
    fallback_reason reason;
};

void install(coordinator &c, flaky_gate &gate, std::vector<milliseconds> &wakes)
{
    c.on_gate(
            [&gate](std::string_view)
            {
                if(++gate.calls <= gate.declines)
                    return upgrade_mint<counted_lane>{gate.reason};
                return upgrade_mint<counted_lane>{std::make_unique<counted_lane>(gate.live), [](std::size_t) { return true; }};
            });
    c.on_receive_gate(
            [&gate](std::string_view, std::string_view)
            {
                if(++gate.calls <= gate.declines)
                    return upgrade_receive{gate.reason};
                return upgrade_receive{[lane = std::make_unique<counted_lane>(gate.live)](std::uint64_t) { (void)lane; }};
            });
    c.set_retry_scheduler([&wakes](milliseconds delay) { wakes.push_back(delay); });
    c.set_topic_hint("alpha", plexus::io::dispatch_hint::frequent);
}

std::vector<milliseconds> backoff(std::uint32_t count)
{
    std::vector<milliseconds> out;
    for(milliseconds d = plexus::io::detail::k_retry_floor; out.size() < count; d *= plexus::io::detail::k_retry_factor)
        out.push_back(d);
    return out;
}

}

TEST_CASE("medium_coordinator_retry a transient decline is re-attempted with growing backoff until the lane engages", "[shm][medium_coordinator_retry]")
{
    const demand_role role = GENERATE(demand_role::publisher, demand_role::subscriber);
    co_host_registry reg;
    coordinator c{reg};
    flaky_gate gate{3, fallback_reason::transient};
    std::vector<milliseconds> wakes;
    install(c, gate, wakes);

    engage_demand(c, "peer", "alpha", role);
    for(std::uint32_t wake = 1; wake <= 3; ++wake)
    {
        REQUIRE(wakes.size() == wake);
        c.on_retry_due();
        REQUIRE(gate.calls == wake + 1);
    }
    REQUIRE(wakes == backoff(3));
    REQUIRE(gate.live == 1);

    c.on_retry_due();
    engage_demand(c, "peer", "alpha", role);
    REQUIRE(gate.calls == 4);
    REQUIRE(wakes.size() == 3);
}

TEST_CASE("medium_coordinator_retry a gate that stays transient stops at the attempt cap", "[shm][medium_coordinator_retry]")
{
    co_host_registry reg;
    coordinator c{reg};
    flaky_gate gate{UINT32_MAX, fallback_reason::transient};
    std::vector<milliseconds> wakes;
    install(c, gate, wakes);

    c.on_edge("peer", "alpha", demand_transition::up, demand_role::publisher);
    for(std::size_t fired = 0; fired < wakes.size(); ++fired)
        c.on_retry_due();
    REQUIRE(wakes == backoff(plexus::io::detail::k_retry_attempts));
    REQUIRE(gate.calls == plexus::io::detail::k_retry_attempts + 1);
    REQUIRE(!c.companion_for("peer", "alpha", 1).fits);
}

TEST_CASE("medium_coordinator_retry a permanent decline is never re-attempted", "[shm][medium_coordinator_retry]")
{
    co_host_registry reg;
    coordinator c{reg};
    flaky_gate gate{UINT32_MAX, fallback_reason::incompatible_layout};
    std::vector<milliseconds> wakes;
    install(c, gate, wakes);

    c.on_edge("peer", "alpha", demand_transition::up, demand_role::publisher);
    c.on_retry_due();
    REQUIRE(wakes.empty());
    REQUIRE(gate.calls == 1);
}

TEST_CASE("medium_coordinator_retry peer death, medium loss and the down-edge each cancel the pending re-attempt", "[shm][medium_coordinator_retry]")
{
    co_host_registry reg;
    coordinator c{reg};
    flaky_gate gate{1, fallback_reason::transient};
    std::vector<milliseconds> wakes;
    install(c, gate, wakes);
    c.on_edge("peer", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(wakes.size() == 1);

    SECTION("peer death")
    {
        c.on_peer_dead("peer");
    }
    SECTION("medium loss")
    {
        c.on_medium_lost("alpha");
    }
    SECTION("down-edge")
    {
        c.on_edge("peer", "alpha", demand_transition::down, demand_role::publisher);
    }
    c.on_retry_due();
    REQUIRE(gate.calls == 1);
    REQUIRE(gate.live == 0);
}

TEST_CASE("medium_coordinator_retry the table ends a transient pair at the cap and a permanent one at once", "[shm][medium_coordinator_retry]")
{
    plexus::io::detail::upgrade_retry table;
    const plexus::io::detail::retry_key key{"peer", "alpha", demand_role::publisher};
    table.defer("peer", "alpha", demand_role::publisher);
    table.defer("peer", "alpha", demand_role::publisher);
    for(std::uint32_t attempt = 1; attempt < plexus::io::detail::k_retry_attempts; ++attempt)
        REQUIRE(table.settle(key, fallback_reason::transient) == fallback_reason::transient);
    REQUIRE(table.settle(key, fallback_reason::transient) == fallback_reason::retries_exhausted);
    REQUIRE(table.empty());

    table.defer("peer", "alpha", demand_role::publisher);
    REQUIRE(table.settle(key, fallback_reason::incompatible_layout) == fallback_reason::incompatible_layout);
    REQUIRE(table.empty());
}
