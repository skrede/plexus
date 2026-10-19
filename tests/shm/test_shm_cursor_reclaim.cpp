#include "support/noop_notifier.h"
#include "support/region_names.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/testing/platform.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>

// A producer reclaims, on a congested reliable send, the cursor of a consumer whose process is gone, proven by the
// cursor's slot lock on the region; a live consumer, however slow, and the producer's own cursor keep theirs.

namespace {

using plexus::shm::ring_role;
using plexus::shm::slot_hold;
using plexus::shm::loan_status;
using plexus::shm::cursor_slot;
using plexus::shm::acquire_result;
using plexus::shm::ring_direction;
using broker_type   = plexus::native::shm_region_broker;
using registry_type = plexus::shm::shm_topic_registry<broker_type, plexus::testing::noop_notifier>;

std::string fresh_fqn(const char *tag)
{
    return std::string("cursor_reclaim.") + tag + "." + std::to_string(plexus::testing::process_id());
}

// One process's registry on the ring, sending reliable and blocking.
struct participant
{
    participant(broker_type &broker, std::string topic)
            : reg(broker, plexus::io::reliability::reliable, plexus::io::congestion::block)
            , fqn(std::move(topic))
    {
    }

    acquire_result acquire(ring_role role)
    {
        return reg.acquire(fqn, ring_direction::request, plexus::node_id{}, role, 64);
    }

    loan_status send()
    {
        const std::array<std::byte, 8> payload{};
        return reg.channel_for(fqn, ring_direction::request, plexus::node_id{})->send(payload);
    }

    void drain()
    {
        registry_type::deliver_fn discard = [](plexus::wire_bytes<plexus::shm::shm_slot_owner>) {};
        reg.channel_for(fqn, ring_direction::request, plexus::node_id{})->drain(discard);
    }

    registry_type reg;
    std::string fqn;
};

// The ring as a reader process maps it, through its own region handles.
struct ring_view
{
    ring_view(broker_type &broker, const std::string &fqn)
            : control()
            , slab()
            , ring()
    {
        const std::string name = plexus::testing::region_name(fqn, plexus::node_id{});
        REQUIRE(broker.attach(name, control) == plexus::shm::region_status::ok);
        REQUIRE(broker.attach(name + ".s", slab) == plexus::shm::region_status::ok);
        REQUIRE(plexus::shm::broadcast_ring::attach(control.bytes(), slab.bytes(), plexus::node_id{}, ring) == loan_status::ok);
    }

    std::uint32_t active(std::uint32_t index)
    {
        return ring.header().cursors[index].active.load(std::memory_order_acquire);
    }

    broker_type::region_handle control;
    broker_type::region_handle slab;
    plexus::shm::broadcast_ring ring;
};

// A reader that holds cursor 0's slot, claims the cursor at the tail and dies: its descriptors close, which frees the
// slot, and nothing unregisters the cursor.
void leave_dead_cursor(broker_type &broker, const std::string &fqn)
{
    ring_view dead{broker, fqn};
    REQUIRE(dead.control.hold_slot(cursor_slot(0)) == slot_hold::held);
    REQUIRE(dead.ring.claim_cursor(0));
    dead.ring.publish_cursor(0, dead.ring.tail_position());
}

bool sends_ok(participant &producer, std::uint64_t count)
{
    for(std::uint64_t i = 0; i < count; ++i)
        if(producer.send() != loan_status::ok)
            return false;
    return true;
}

}

TEST_CASE("cursor_reclaim a congested reliable send reclaims a dead consumer's cursor and proceeds", "[shm][cursor_reclaim]")
{
    broker_type broker;
    participant producer{broker, fresh_fqn("dead")};
    REQUIRE(producer.acquire(ring_role::producer) == acquire_result::created);
    leave_dead_cursor(broker, producer.fqn);
    ring_view view{broker, producer.fqn};
    REQUIRE(view.active(0) == 1);

    CHECK(sends_ok(producer, view.ring.cell_count() * 2));
    CHECK(view.active(0) == 0);
}

TEST_CASE("cursor_reclaim a consumer joining after a death passes over the dead consumer's cursor", "[shm][cursor_reclaim]")
{
    broker_type broker;
    participant producer{broker, fresh_fqn("join")};
    REQUIRE(producer.acquire(ring_role::producer) == acquire_result::created);
    leave_dead_cursor(broker, producer.fqn);
    ring_view view{broker, producer.fqn};

    participant consumer{broker, producer.fqn};
    REQUIRE(consumer.acquire(ring_role::consumer) == acquire_result::attached);
    CHECK(view.active(0) == 1);
    CHECK(view.active(1) == 1);
    CHECK(view.control.slot_held_elsewhere(cursor_slot(1)));
}

TEST_CASE("cursor_reclaim a live consumer that has not drained keeps its cursor and the send stays congested", "[shm][cursor_reclaim]")
{
    broker_type broker;
    participant producer{broker, fresh_fqn("live")};
    participant consumer{broker, producer.fqn};
    REQUIRE(producer.acquire(ring_role::producer) == acquire_result::created);
    REQUIRE(consumer.acquire(ring_role::consumer) == acquire_result::attached);
    ring_view view{broker, producer.fqn};

    REQUIRE(sends_ok(producer, view.ring.cell_count()));
    CHECK(producer.send() == loan_status::congested);
    CHECK(view.active(0) == 1);

    consumer.drain();
    CHECK(producer.send() == loan_status::ok);
}

TEST_CASE("cursor_reclaim the producer never reclaims the cursor its own entry reads with", "[shm][cursor_reclaim]")
{
    broker_type broker;
    participant node{broker, fresh_fqn("own")};
    REQUIRE(node.acquire(ring_role::both) == acquire_result::created);
    ring_view view{broker, node.fqn};

    REQUIRE(sends_ok(node, view.ring.cell_count()));
    CHECK(node.send() == loan_status::congested);
    CHECK(view.active(0) == 1);
}

TEST_CASE("cursor_reclaim reclaiming an inactive, already reclaimed or live cursor changes nothing", "[shm][cursor_reclaim]")
{
    broker_type broker;
    participant producer{broker, fresh_fqn("again")};
    REQUIRE(producer.acquire(ring_role::producer) == acquire_result::created);
    leave_dead_cursor(broker, producer.fqn);
    ring_view view{broker, producer.fqn};
    REQUIRE(sends_ok(producer, view.ring.cell_count() + 1));

    auto dead = [](std::uint32_t) { return false; };
    CHECK_FALSE(view.ring.reclaim_cursor(0, dead));
    CHECK_FALSE(view.ring.reclaim_cursor(5, dead));
    CHECK(view.active(0) == 0);

    participant consumer{broker, producer.fqn};
    REQUIRE(consumer.acquire(ring_role::consumer) == acquire_result::attached);
    auto proof = [&view](std::uint32_t index) { return view.control.slot_held_elsewhere(cursor_slot(index)); };
    CHECK_FALSE(view.ring.reclaim_cursor(0, proof));
    CHECK(view.active(0) == 1);
}
