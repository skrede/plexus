#include "support/member_lanes.h"
#include "support/region_names.h"
#include "support/noop_notifier.h"
#include "support/delaying_broker.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/fallback_notifier.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/upgrade_channel.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace {

using plexus::shm::arm_status;
using plexus::io::fallback_counts;
using plexus::io::fallback_reason;
using plexus::shm::region_status;
using plexus::testing::full_broker;
using plexus::testing::member_lanes;
using plexus::testing::delaying_broker;
using demote_fn = plexus::detail::move_only_function<void(arm_status)>;

constexpr std::uint64_t k_cells         = 16;
constexpr std::uint64_t k_slot          = 64;
constexpr std::uint64_t k_previous_ring = 0x50'4c'58'52'4e'47'34'00ull; // "PLXRNG4\0"
const std::string k_fqn                 = "medium_coordinator_member.alpha";

// What each wake answers at arm; demote fires the primary's runtime demotion the way the executor runs its report.
struct legs
{
    legs(arm_status primary, arm_status secondary)
            : primary(primary)
            , secondary(secondary)
            , held()
    {
    }

    void demote(arm_status why)
    {
        demote_fn report = std::exchange(held, nullptr);
        report(why);
    }

    arm_status primary;
    arm_status secondary;
    demote_fn held;
};

template<bool Primary>
struct leg
{
    explicit leg(legs &l) noexcept
            : wake(l)
    {
    }

    void signal() noexcept
    {
    }

    arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>, demote_fn demote)
    {
        wake.held = std::move(demote);
        return wake.primary;
    }

    arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>)
    {
        return wake.secondary;
    }

    void disarm() noexcept
    {
        if constexpr(Primary)
            wake.held = nullptr;
    }

    legs &wake;
};

using layered = plexus::shm::fallback_notifier<leg<true>, leg<false>>;

using noop_registry = plexus::shm::shm_topic_registry<delaying_broker, plexus::testing::noop_notifier>;

fallback_counts counted(fallback_reason reason)
{
    fallback_counts out{};
    out[static_cast<std::size_t>(reason)] = 1;
    return out;
}

plexus::detail::move_only_function<void(std::optional<layered> &, plexus::shm::control_header_t &, std::string_view)> bind_legs(legs &l)
{
    return [&l](std::optional<layered> &slot, plexus::shm::control_header_t &, std::string_view) { slot.emplace(l); };
}

}

TEST_CASE("medium_coordinator_member a ring stamped with the previous layout magic is counted as incompatible_layout", "[shm][medium_coordinator_member]")
{
    delaying_broker broker;
    const std::string name = plexus::testing::region_name(k_fqn, plexus::node_id{});
    delaying_broker::region_handle control, slab;
    REQUIRE(broker.create(name, plexus::shm::control_region_bytes(k_cells), {}, control) == region_status::ok);
    REQUIRE(broker.create(name + ".s", plexus::shm::slab_region_bytes(k_cells, k_slot), {}, slab) == region_status::ok);
    plexus::shm::broadcast_ring ring;
    REQUIRE(plexus::shm::broadcast_ring::create(control.bytes(), slab.bytes(), k_cells, k_slot, plexus::node_id{}, ring) == plexus::shm::loan_status::ok);
    ring.header().magic.store(k_previous_ring);

    member_lanes<delaying_broker, plexus::testing::noop_notifier> m{broker, k_fqn};
    REQUIRE(m.lanes.fallbacks() == counted(fallback_reason::incompatible_layout));
    REQUIRE_FALSE(m.engaged());
}

TEST_CASE("medium_coordinator_member a host with no memory to back the ring is counted as no_space", "[shm][medium_coordinator_member]")
{
    full_broker broker;
    member_lanes<full_broker, plexus::testing::noop_notifier> m{broker, k_fqn};
    REQUIRE(m.lanes.fallbacks() == counted(fallback_reason::no_space));
    REQUIRE_FALSE(m.engaged());
}

TEST_CASE("medium_coordinator_member a control region that vanishes before the join, or a ring with no reader slot left, is counted by its reason", "[shm][medium_coordinator_member]")
{
    delaying_broker broker;
    noop_registry peer(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    REQUIRE(peer.acquire(k_fqn, plexus::shm::ring_direction::request, plexus::node_id{}, 0, plexus::shm::ring_geometry_mode::reliable_preserving, 1,
                         plexus::shm::acquire_mode::join_live) == plexus::shm::acquire_result::created);
    const bool vanished       = GENERATE(true, false);
    const fallback_reason why = vanished ? fallback_reason::not_found : fallback_reason::consumer_capacity;
    if(vanished)
        broker.withhold_until(plexus::testing::region_name(k_fqn, plexus::node_id{}), delaying_broker::clock::time_point::max());

    member_lanes<delaying_broker, plexus::testing::noop_notifier> m{broker, k_fqn};
    REQUIRE(m.lanes.fallbacks() == counted(why));
    REQUIRE_FALSE(m.engaged());
}

TEST_CASE("medium_coordinator_member a wake that falls to its secondary at arm is counted once and the lane engages", "[shm][medium_coordinator_member]")
{
    delaying_broker broker;
    legs wake{arm_status::denied, arm_status::armed};
    member_lanes<delaying_broker, layered> m{broker, k_fqn, bind_legs(wake)};
    REQUIRE(m.lanes.fallbacks() == counted(fallback_reason::notifier_demoted_denied));
    REQUIRE(m.engaged());
}

TEST_CASE("medium_coordinator_member a runtime demotion is counted while the lane stays engaged, a lost waiter drops it", "[shm][medium_coordinator_member]")
{
    delaying_broker broker;
    const bool secondary_arms = GENERATE(true, false);
    legs wake{arm_status::armed, secondary_arms ? arm_status::armed : arm_status::denied};
    member_lanes<delaying_broker, layered> m{broker, k_fqn, bind_legs(wake)};
    REQUIRE(m.lanes.fallbacks() == fallback_counts{});
    REQUIRE(m.engaged());

    wake.demote(arm_status::unsupported);
    REQUIRE(m.lanes.fallbacks() == counted(secondary_arms ? fallback_reason::notifier_demoted_unsupported : fallback_reason::waiter_lost));
    REQUIRE(m.engaged() == secondary_arms);
}
