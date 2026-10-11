#include "notifier_legs.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/fallback_notifier.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <string>
#include <optional>
#include <string_view>

namespace {

using plexus::shm::arm_status;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using lost_notifier = plexus::shm::fallback_notifier<plexus::testing::demoting_primary<plexus::asio::asio_policy>, plexus::testing::denied_floor>;
using registry      = plexus::shm::shm_topic_registry<plexus::native::shm_region_broker, lost_notifier>;

registry::notifier_binder lost_binder(::asio::io_context &io)
{
    return [&io](std::optional<lost_notifier> &slot, plexus::shm::control_header_t &header, std::string_view name) { slot.emplace(io, header, name); };
}

std::string fresh_fqn(const char *tag)
{
    return std::string("registry_loss.") + tag + "." + std::to_string(plexus::testing::process_id());
}

bool region_exists(const std::string &fqn)
{
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle handle;
    return broker.attach(plexus::shm::region_name_for(fqn, ring_direction::request), handle) == plexus::shm::region_status::ok;
}

}

TEST_CASE("registry_loss a waiter lost at runtime fails every acquire of its key until the last release", "[shm][registry_loss][!shouldfail]")
{
    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    registry reg{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, lost_binder(io)};
    const std::string fqn = fresh_fqn("latch");

    REQUIRE(reg.acquire(fqn, ring_direction::request, 0) == acquire_result::created);
    io.run();

    CHECK(reg.acquire(fqn, ring_direction::request, 0) == acquire_result::failed);
    CHECK(reg.last_acquire_failure().bound == plexus::shm::acquire_bound::notifier);
    CHECK(reg.last_acquire_failure().arm == arm_status::denied);
    CHECK(reg.channel_for(fqn, ring_direction::request) == nullptr);

    reg.release(fqn, ring_direction::request);
    CHECK(reg.live_count() == 0);
    CHECK_FALSE(region_exists(fqn));

    REQUIRE(reg.acquire(fqn, ring_direction::request, 0) == acquire_result::created);
    reg.release(fqn, ring_direction::request);
}
