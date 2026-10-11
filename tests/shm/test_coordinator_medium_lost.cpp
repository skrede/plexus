#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

using plexus::io::demand_role;
using plexus::io::dispatch_hint;
using plexus::io::upgrade_mint;
using plexus::io::upgrade_receive;
using plexus::io::demand_transition;

struct co_host_registry
{
    bool same_host_for(std::string_view) const noexcept
    {
        return true;
    }
};

// One live lane; the count falls when the coordinator drops it.
class counted_lane
{
public:
    explicit counted_lane(std::int32_t &live)
            : m_live(live)
    {
        ++m_live;
    }

    ~counted_lane()
    {
        --m_live;
    }

    counted_lane(const counted_lane &)            = delete;
    counted_lane &operator=(const counted_lane &) = delete;

private:
    std::int32_t &m_live;
};

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
    c.on_receive_gate([&live](std::string_view, std::string_view) { return upgrade_receive{[lane = std::make_unique<counted_lane>(live.receives)] { (void)lane; }}; });
    c.set_topic_hint("alpha", dispatch_hint::frequent);
    c.set_topic_hint("beta", dispatch_hint::frequent);
}

}

TEST_CASE("coordinator_loss a lost medium drops every peer's send and receive lanes for its fqn only", "[shm][coordinator_loss]")
{
    co_host_registry reg;
    coordinator c{reg};
    lanes live;
    install_gates(c, live);
    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    c.on_edge("node-b", "alpha", demand_transition::up, demand_role::subscriber);
    c.on_edge("node-a", "beta", demand_transition::up, demand_role::publisher);
    REQUIRE(live.sends == 2);
    REQUIRE(live.receives == 1);

    c.on_medium_lost("alpha");

    CHECK(live.sends == 1);
    CHECK(live.receives == 0);
    CHECK(c.companion_for("node-a", "alpha", 1) == nullptr);
    CHECK(c.companion_for("node-a", "beta", 1) != nullptr);
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

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);

    CHECK(live.mints == 2);
    CHECK(live.sends == 1);
    CHECK(c.companion_for("node-a", "alpha", 1) != nullptr);
}
