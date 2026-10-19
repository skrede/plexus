#include "support/noop_notifier.h"
#include "support/region_names.h"
#include "support/delaying_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <cstdint>

// A ring's name carries its publisher and its universe, so two universes on one host never share a ring for one topic
// and one publisher, and two publishers of one topic never share a ring or a registry entry.

namespace {

using plexus::shm::acquire_mode;
using plexus::shm::ring_role;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using plexus::testing::delaying_broker;
using registry = plexus::shm::shm_topic_registry<delaying_broker, plexus::testing::noop_notifier>;

constexpr std::uint32_t k_universe_a = 0x58E1B347u;
constexpr std::uint32_t k_universe_b = 0x2B0F9A01u;
const std::string k_fqn              = "topic.universe";

acquire_result join(registry &r, const plexus::node_id &publisher, ring_role role)
{
    return r.acquire(k_fqn, ring_direction::request, publisher, role, 64, plexus::io::ring_geometry_mode::reliable_preserving, 0, acquire_mode::join_live);
}

void send_n(registry &r, const plexus::node_id &publisher, std::uint32_t n)
{
    const std::array<std::byte, 4> payload{};
    for(std::uint32_t i = 0; i < n; ++i)
        REQUIRE(r.channel_for(k_fqn, ring_direction::request, publisher)->send(payload) == plexus::shm::loan_status::ok);
}

std::uint32_t delivered(registry &r, const plexus::node_id &publisher)
{
    std::uint32_t count = 0;
    r.set_consumer_sink(k_fqn, ring_direction::request, publisher, [&count](plexus::wire_bytes<plexus::shm::shm_slot_owner>) { ++count; });
    r.clear_consumer_sink(k_fqn, ring_direction::request, publisher);
    return count;
}

}

TEST_CASE("region_universe two universes with one fqn and one publisher name two rings", "[shm][region_universe]")
{
    const plexus::node_id publisher = plexus::testing::test_publisher();
    REQUIRE(plexus::testing::region_name(k_fqn, publisher, k_universe_a) != plexus::testing::region_name(k_fqn, publisher, k_universe_b));
}

TEST_CASE("region_universe an acquire under one universe never attaches the ring of another", "[shm][region_universe]")
{
    delaying_broker broker;
    const plexus::node_id publisher = plexus::testing::test_publisher();
    registry a{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, registry::default_notifier_binder(), plexus::shm::k_max_ring_slab_bytes, {}, k_universe_a};
    registry a_peer{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, registry::default_notifier_binder(), plexus::shm::k_max_ring_slab_bytes, {}, k_universe_a};
    registry b{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    b.bind_universe(k_universe_b);

    REQUIRE(join(a, publisher, ring_role::producer) == acquire_result::created);
    REQUIRE(join(b, publisher, ring_role::both) == acquire_result::created);
    REQUIRE(join(a_peer, publisher, ring_role::consumer) == acquire_result::attached);

    send_n(a, publisher, 1);
    CHECK(delivered(a_peer, publisher) == 1);
    CHECK(delivered(b, publisher) == 0);
}

TEST_CASE("region_universe two publishers of one fqn own two rings, two entries and two sinks", "[shm][region_universe]")
{
    delaying_broker broker;
    registry r{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    const plexus::node_id first  = plexus::node_id_from_name("universe.publisher.first");
    const plexus::node_id second = plexus::node_id_from_name("universe.publisher.second");

    REQUIRE(join(r, first, ring_role::both) == acquire_result::created);
    REQUIRE(join(r, second, ring_role::both) == acquire_result::created);
    REQUIRE(r.live_count() == 2);
    REQUIRE(r.channel_for(k_fqn, ring_direction::request, first) != r.channel_for(k_fqn, ring_direction::request, second));

    send_n(r, first, 1);
    send_n(r, second, 2);
    CHECK(delivered(r, first) == 1);
    CHECK(delivered(r, second) == 2);
}
