#include "plexus/shm/arm_status.h"
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

namespace {

struct denied_notifier
{
    void signal() noexcept
    {
    }

    plexus::shm::arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>)
    {
        return plexus::shm::arm_status::denied;
    }

    void disarm() noexcept
    {
    }
};

static_assert(plexus::shm::notifier<denied_notifier>);

using registry = plexus::shm::shm_topic_registry<plexus::native::shm_region_broker, denied_notifier>;

std::string fresh_fqn(const char *tag)
{
    return std::string("registry_arm.") + tag + "." + std::to_string(plexus::testing::process_id());
}

}

TEST_CASE("registry_arm a notifier that fails to arm fails the acquire", "[shm][registry_arm][!shouldfail]")
{
    plexus::native::shm_region_broker broker;
    registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};

    const plexus::shm::acquire_result result = reg.acquire(fresh_fqn("denied"), plexus::shm::ring_direction::request, 0);

    REQUIRE(result == plexus::shm::acquire_result::failed);
    REQUIRE(reg.live_count() == 0);
}
