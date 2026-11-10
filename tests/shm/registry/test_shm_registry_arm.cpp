#include "support/region_names.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace {

using plexus::shm::arm_status;

// Arms with the outcome it was built with; the default one cannot arm.
struct outcome_notifier
{
    outcome_notifier() noexcept
            : outcome(arm_status::denied)
    {
    }

    explicit outcome_notifier(arm_status o) noexcept
            : outcome(o)
    {
    }

    void signal() noexcept
    {
    }

    arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>)
    {
        return outcome;
    }

    void disarm() noexcept
    {
    }

    arm_status outcome;
};

static_assert(plexus::shm::notifier<outcome_notifier>);

using registry = plexus::shm::shm_topic_registry<plexus::native::shm_region_broker, outcome_notifier>;

std::string fresh_fqn(const char *tag)
{
    return std::string("registry_arm.") + tag + "." + std::to_string(plexus::testing::process_id());
}

bool region_exists(const std::string &fqn)
{
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle handle;
    return broker.attach(plexus::testing::region_name(fqn, plexus::node_id{}), handle) == plexus::shm::region_status::ok;
}

}

TEST_CASE("registry_arm a notifier that fails to arm fails the acquire", "[shm][registry_arm]")
{
    plexus::native::shm_region_broker broker;
    registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    const std::string fqn = fresh_fqn("denied");

    const plexus::shm::acquire_result result = reg.acquire(fqn, plexus::shm::ring_direction::request, plexus::node_id{}, plexus::shm::ring_role::both, 0);

    REQUIRE(result == plexus::shm::acquire_result::failed);
    REQUIRE(reg.live_count() == 0);
    CHECK(reg.last_acquire_failure().bound == plexus::shm::acquire_bound::notifier);
    CHECK(reg.last_acquire_failure().arm == arm_status::denied);
    CHECK_FALSE(region_exists(fqn));
}

TEST_CASE("registry_arm a later acquire that arms clears the arm failure", "[shm][registry_arm]")
{
    plexus::native::shm_region_broker broker;
    arm_status next                  = arm_status::unsupported;
    registry::notifier_binder binder = [&next](std::optional<outcome_notifier> &slot, plexus::shm::control_header_t &, std::string_view) { slot.emplace(next); };
    registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, std::move(binder)};
    const std::string fqn = fresh_fqn("reset");

    REQUIRE(reg.acquire(fqn, plexus::shm::ring_direction::request, plexus::node_id{}, plexus::shm::ring_role::both, 0) == plexus::shm::acquire_result::failed);
    CHECK(reg.last_acquire_failure().arm == arm_status::unsupported);

    next = arm_status::armed;
    REQUIRE(reg.acquire(fqn, plexus::shm::ring_direction::request, plexus::node_id{}, plexus::shm::ring_role::both, 0) == plexus::shm::acquire_result::created);
    CHECK(reg.live_count() == 1);
    CHECK(reg.last_acquire_failure().bound == plexus::shm::acquire_bound::none);
    CHECK(reg.last_acquire_failure().arm == arm_status::armed);
    reg.release(fqn, plexus::shm::ring_direction::request, plexus::node_id{}, plexus::shm::ring_role::both);
}
