#include "support/coordinator_lanes.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
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
using plexus::io::fallback_counts;
using plexus::io::fallback_reason;
using plexus::io::upgrade_receive;
using plexus::io::demand_transition;
using std::chrono::milliseconds;
using plexus::io::companion_event;
using plexus::testing::engage_demand;
using plexus::testing::co_host_registry;

struct lane
{
};

using coordinator = plexus::io::upgrade_coordinator<co_host_registry, lane>;

// Declines with reason for the first declines calls, then engages.
struct gate
{
    gate(std::uint32_t declines, fallback_reason reason)
            : calls(0)
            , declines(declines)
            , reason(reason)
    {
    }

    std::uint32_t calls;
    std::uint32_t declines;
    fallback_reason reason;
};

void install(coordinator &c, gate &g)
{
    c.on_gate(
            [&g](std::string_view)
            {
                if(++g.calls <= g.declines)
                    return upgrade_mint<lane>{g.reason};
                return upgrade_mint<lane>{std::make_unique<lane>(), [](std::size_t) { return true; }};
            });
    c.on_receive_gate(
            [&g](std::string_view, std::string_view)
            {
                if(++g.calls <= g.declines)
                    return upgrade_receive{g.reason};
                return upgrade_receive{[held = std::make_unique<lane>()] { (void)held; }};
            });
    c.set_topic_hint("alpha", plexus::io::dispatch_hint::frequent);
}

void drive_retries(coordinator &c, std::vector<milliseconds> &wakes)
{
    c.set_retry_scheduler([&wakes](milliseconds delay) { wakes.push_back(delay); });
    c.on_edge("peer", "alpha", demand_transition::up, demand_role::publisher);
    for(std::size_t fired = 0; fired < wakes.size(); ++fired)
        c.on_retry_due();
}

fallback_counts counted(fallback_reason reason, std::uint64_t times)
{
    fallback_counts out{};
    out[static_cast<std::size_t>(reason)] = times;
    return out;
}

}

TEST_CASE("medium_coordinator_reasons a declined lane counts exactly its reason and keeps the wire", "[shm][medium_coordinator_reasons]")
{
    const fallback_reason reason = GENERATE(fallback_reason::transient, fallback_reason::incompatible_layout, fallback_reason::slab_ceiling, fallback_reason::os_allocator,
                                            fallback_reason::notifier_denied, fallback_reason::notifier_unsupported, fallback_reason::notifier_exhausted,
                                            fallback_reason::consumer_capacity, fallback_reason::not_found, fallback_reason::no_space);
    const demand_role role       = GENERATE(demand_role::publisher, demand_role::subscriber);
    co_host_registry reg;
    coordinator c{reg};
    gate g{UINT32_MAX, reason};
    install(c, g);

    engage_demand(c, "peer", "alpha", role);
    REQUIRE(c.fallbacks() == counted(reason, 1));
    REQUIRE(c.companion_for("peer", "alpha", 1) == nullptr);
}

TEST_CASE("medium_coordinator_reasons a transient decline that later engages counts each transient attempt and no permanent reason", "[shm][medium_coordinator_reasons]")
{
    co_host_registry reg;
    coordinator c{reg};
    gate g{3, fallback_reason::transient};
    install(c, g);
    std::vector<milliseconds> wakes;

    drive_retries(c, wakes);
    c.on_companion("peer", "alpha", companion_event::ready);
    REQUIRE(g.calls == 4);
    REQUIRE(c.fallbacks() == counted(fallback_reason::transient, 3));
    REQUIRE(c.companion_for("peer", "alpha", 1) != nullptr);
}

TEST_CASE("medium_coordinator_reasons a pair that stays transient counts every attempt and retries_exhausted once", "[shm][medium_coordinator_reasons]")
{
    co_host_registry reg;
    coordinator c{reg};
    gate g{UINT32_MAX, fallback_reason::transient};
    install(c, g);
    std::vector<milliseconds> wakes;

    drive_retries(c, wakes);
    fallback_counts expected                                               = counted(fallback_reason::transient, plexus::io::detail::k_retry_attempts + 1);
    expected[static_cast<std::size_t>(fallback_reason::retries_exhausted)] = 1;
    REQUIRE(c.fallbacks() == expected);
    c.on_retry_due();
    REQUIRE(c.fallbacks() == expected);
}

TEST_CASE("medium_coordinator_reasons a demotion of an engaged lane is counted while the lane stays engaged", "[shm][medium_coordinator_reasons]")
{
    co_host_registry reg;
    coordinator c{reg};
    gate g{0, fallback_reason::none};
    install(c, g);
    REQUIRE(c.fallbacks() == fallback_counts{});

    engage_demand(c, "peer", "alpha", demand_role::publisher);
    c.count_fallback(fallback_reason::notifier_demoted_denied);
    c.count_fallback(fallback_reason::none);
    REQUIRE(c.fallbacks() == counted(fallback_reason::notifier_demoted_denied, 1));
    REQUIRE(c.companion_for("peer", "alpha", 1) != nullptr);
}
