#include "support/region_names.h"
#include "support/noop_notifier.h"
#include "support/delaying_broker.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <cstdint>

namespace {

using plexus::shm::ring_role;
using plexus::shm::acquire_bound;
using plexus::shm::region_status;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using plexus::testing::delaying_broker;
using registry = plexus::shm::shm_topic_registry<delaying_broker, plexus::testing::noop_notifier>;

constexpr std::uint64_t k_cells         = 16;
constexpr std::uint64_t k_slot          = 64;
constexpr std::uint64_t k_foreign_magic = 0x46'4f'52'45'49'47'4e'00ull; // "FOREIGN\0"

std::string fresh_fqn(const char *tag)
{
    return std::string("registry_join.") + tag + "." + std::to_string(plexus::testing::process_id());
}

// A co-host peer's ring, minted as far as the test lets it get: both regions exist, and the header is stamped only
// when asked.
struct peer_ring
{
    peer_ring(delaying_broker &broker, const std::string &fqn, bool stamp)
            : ctrl_name(plexus::testing::region_name(fqn, plexus::node_id{}))
    {
        REQUIRE(broker.create(ctrl_name, plexus::shm::control_region_bytes(k_cells), {}, control) == region_status::ok);
        REQUIRE(broker.create(ctrl_name + ".s", plexus::shm::slab_region_bytes(k_cells, k_slot), {}, slab) == region_status::ok);
        if(stamp)
            REQUIRE(plexus::shm::broadcast_ring::create(control.bytes(), slab.bytes(), k_cells, k_slot, plexus::node_id{}, ring) == plexus::shm::loan_status::ok);
    }

    std::string ctrl_name;
    plexus::shm::broadcast_ring ring;
    delaying_broker::region_handle slab;
    delaying_broker::region_handle control;
};

acquire_result join(registry &reg, const std::string &fqn)
{
    return reg.acquire(fqn, ring_direction::request, plexus::node_id{}, ring_role::consumer, k_slot);
}

}

TEST_CASE("registry_join a control region whose slab is not there yet is a classified join failure", "[shm][registry_join]")
{
    delaying_broker broker;
    registry reg(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    const std::string fqn = fresh_fqn("slab");
    peer_ring peer(broker, fqn, true);
    broker.withhold_until(peer.ctrl_name + ".s", delaying_broker::clock::time_point::max());

    REQUIRE(join(reg, fqn) == acquire_result::failed);
    REQUIRE(reg.last_acquire_failure().bound == acquire_bound::transient_join);
}

TEST_CASE("registry_join a header its creator has not stamped yet is a classified join failure", "[shm][registry_join]")
{
    delaying_broker broker;
    registry reg(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    const std::string fqn = fresh_fqn("unstamped");
    peer_ring peer(broker, fqn, false);

    REQUIRE(join(reg, fqn) == acquire_result::failed);
    REQUIRE(reg.last_acquire_failure().bound == acquire_bound::transient_join);
}

TEST_CASE("registry_join a header stamped with a foreign layout is a classified join failure", "[shm][registry_join]")
{
    delaying_broker broker;
    registry reg(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    const std::string fqn = fresh_fqn("foreign");
    peer_ring peer(broker, fqn, true);
    peer.ring.header().magic.store(k_foreign_magic);

    REQUIRE(join(reg, fqn) == acquire_result::failed);
    REQUIRE(reg.last_acquire_failure().bound == acquire_bound::incompatible);
}

TEST_CASE("registry_join a withheld slab joins once it appears and a repeat acquire only bumps the refcount", "[shm][registry_join]")
{
    delaying_broker broker;
    registry reg(broker, plexus::io::reliability::reliable, plexus::io::congestion::block);
    const std::string fqn = fresh_fqn("late");
    peer_ring peer(broker, fqn, true);
    broker.withhold_until(peer.ctrl_name + ".s", delaying_broker::clock::now() + std::chrono::milliseconds(20));

    REQUIRE(join(reg, fqn) == acquire_result::failed);
    const auto deadline = delaying_broker::clock::now() + std::chrono::seconds(5);
    while(join(reg, fqn) != acquire_result::attached)
        REQUIRE(delaying_broker::clock::now() < deadline);
    REQUIRE(join(reg, fqn) == acquire_result::attached);
    REQUIRE(reg.live_count() == 1);
}
