#include "test_shm_registry_roles_common.h"

#include <array>
#include <cstddef>
#include <cstdint>

using namespace registry_roles_fixture;

TEST_CASE("registry_roles a producer-only acquire registers no cursor, arms no waiter and never gates its sends", "[shm][registry_roles]")
{
    delaying_broker broker;
    participant publisher{broker};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    header_view header{broker};

    CHECK(header.ring.registered_high_water() == 0);
    CHECK(header.active_cursors() == 0);
    CHECK(publisher.log.arms == 0);
    CHECK(publisher.sends_freely());
}

TEST_CASE("registry_roles each consumer acquire adds one cursor and arms its waiter, and its release removes both", "[shm][registry_roles]")
{
    delaying_broker broker;
    participant publisher{broker};
    participant first{broker};
    participant second{broker};
    REQUIRE(publisher.acquire(ring_role::producer) == acquire_result::created);
    header_view header{broker};

    REQUIRE(first.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(header.active_cursors() == 1);
    CHECK(first.log.armed);
    REQUIRE(second.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(header.active_cursors() == 2);

    first.release(ring_role::consumer);
    second.release(ring_role::consumer);
    CHECK(header.active_cursors() == 0);
    CHECK_FALSE(first.log.armed);
    CHECK(first.reg.live_count() == 0);
    CHECK(publisher.log.arms == 0);
}

TEST_CASE("registry_roles releasing the consumer half of a both-role hold drops its cursor and keeps the send", "[shm][registry_roles]")
{
    delaying_broker broker;
    participant node{broker};
    REQUIRE(node.acquire(ring_role::both) == acquire_result::created);
    header_view header{broker};
    CHECK(header.active_cursors() == 1);
    CHECK(node.log.armed);

    node.release(ring_role::consumer);
    CHECK(header.active_cursors() == 0);
    CHECK_FALSE(node.log.armed);
    REQUIRE(node.reg.live_count() == 1);
    CHECK(node.sends_freely());
    node.release(ring_role::producer);
    CHECK(node.reg.live_count() == 0);
}

TEST_CASE("registry_roles a consumer acquire on the producer's own entry adds exactly one cursor and the entry outlives it", "[shm][registry_roles]")
{
    delaying_broker broker;
    participant node{broker};
    REQUIRE(node.acquire(ring_role::producer) == acquire_result::created);
    header_view header{broker};
    REQUIRE(node.acquire(ring_role::consumer) == acquire_result::created);
    CHECK(header.active_cursors() == 1);
    CHECK(node.log.armed);

    const std::array<std::byte, 8> payload{};
    REQUIRE(node.reg.channel_for(k_fqn, ring_direction::request, plexus::node_id{})->send(payload) == loan_status::ok);
    std::uint32_t delivered = 0;
    node.reg.set_consumer_sink(k_fqn, ring_direction::request, plexus::node_id{}, [&delivered](plexus::wire_bytes<plexus::shm::shm_slot_owner>) { ++delivered; });
    CHECK(delivered == 1);

    node.reg.clear_consumer_sink(k_fqn, ring_direction::request, plexus::node_id{});
    node.release(ring_role::consumer);
    CHECK(header.active_cursors() == 0);
    REQUIRE(node.reg.live_count() == 1);
    CHECK(node.sends_freely());
}

TEST_CASE("registry_roles a consumer acquire with no ring under the name fails as not_found and creates nothing", "[shm][registry_roles]")
{
    delaying_broker broker;
    participant reader{broker};

    REQUIRE(reader.acquire(ring_role::consumer) == acquire_result::failed);
    CHECK(reader.reg.last_acquire_failure().bound == plexus::shm::acquire_bound::not_found);
    CHECK(reader.reg.live_count() == 0);
    delaying_broker::region_handle probe;
    CHECK(broker.attach(plexus::testing::region_name(k_fqn, plexus::node_id{}), probe) == plexus::shm::region_status::not_found);
}
