#include "support/noop_notifier.h"
#include "support/delaying_broker.h"

#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/ring_geometry_mode.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>

namespace {

using plexus::shm::acquire_mode;
using plexus::shm::acquire_bound;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using plexus::testing::delaying_broker;
using registry = plexus::shm::shm_topic_registry<delaying_broker, plexus::testing::noop_notifier>;

constexpr std::uint32_t k_slot     = 64;
constexpr std::uint32_t k_capacity = 1;

std::string fresh_fqn(const char *tag)
{
    return std::string("registry_capacity.") + tag + "." + std::to_string(plexus::testing::process_id());
}

acquire_result acquire(registry &reg, const std::string &fqn)
{
    return reg.acquire(fqn, ring_direction::request, plexus::node_id{}, k_slot, plexus::shm::ring_geometry_mode::reliable_preserving, k_capacity, acquire_mode::join_live);
}

}

TEST_CASE("registry_capacity a co-host acquire past the ring's consumer capacity fails instead of attaching a reader that never receives", "[shm][registry_capacity]")
{
    delaying_broker broker;
    registry creator(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    registry joiner(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    const std::string fqn = fresh_fqn("over");

    REQUIRE(acquire(creator, fqn) == acquire_result::created);
    REQUIRE(acquire(joiner, fqn) == acquire_result::failed);
    REQUIRE(joiner.last_acquire_failure().bound == acquire_bound::consumer_capacity);
    REQUIRE(joiner.live_count() == 0);
    REQUIRE(acquire(creator, fqn) == acquire_result::created);
}
