#include "test_shm_ring_core_common.h"

#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/taken_message.h"
#include "plexus/shm/slot_publisher.h"
#include "plexus/shm/slot_subscriber.h"

#include <vector>
#include <cstdint>
#include <cstring>
#include <utility>

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_cells = 8;
constexpr std::uint64_t k_slot  = 64;

struct abandon_ring
{
    abandon_ring()
            : control(control_region_bytes(k_cells))
            , slab(slab_region_bytes(k_cells, k_slot))
            , ring()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, plexus::node_id{}, ring) == loan_status::ok);
    }

    backing_region control;
    backing_region slab;
    broadcast_ring ring;
};

slot_publisher reliable_on(broadcast_ring &ring)
{
    return slot_publisher{ring, plexus::io::reliability::reliable, plexus::io::congestion::block};
}

slot_publisher best_effort_on(broadcast_ring &ring)
{
    return slot_publisher{ring, plexus::io::reliability::best_effort, plexus::io::congestion::drop_oldest};
}

void publish_loan(slot_publisher &pub, loaned_buffer &&slot, std::uint32_t value)
{
    std::memcpy(slot.bytes().data(), &value, sizeof(value));
    slot.set_filled(sizeof(value));
    REQUIRE(pub.publish(std::move(slot)) == loan_status::ok);
}

void publish_value(slot_publisher &pub, std::uint32_t value)
{
    loaned_buffer slot;
    REQUIRE(pub.loan(sizeof(value), slot) == loan_status::ok);
    publish_loan(pub, std::move(slot), value);
}

std::vector<std::uint32_t> take_values(slot_subscriber &consumer)
{
    std::vector<std::uint32_t> values;
    taken_message msg;
    while(consumer.take(msg) == loan_status::ok)
    {
        std::uint32_t value = 0;
        std::memcpy(&value, msg.bytes().data(), sizeof(value));
        values.push_back(value);
    }
    return values;
}

}

TEST_CASE("loan_abandon a reliable loan dropped without publishing does not stop later publishes reaching a consumer", "[shm][loan_abandon]")
{
    abandon_ring r;
    slot_subscriber consumer{r.ring, 0};
    slot_publisher pub = reliable_on(r.ring);
    {
        loaned_buffer dropped;
        REQUIRE(pub.loan(k_slot, dropped) == loan_status::ok);
    }
    for(std::uint32_t value = 1; value <= 3; ++value)
        publish_value(pub, value);

    CHECK(take_values(consumer) == std::vector<std::uint32_t>{1, 2, 3});
}

TEST_CASE("loan_abandon a moved-from loan abandons nothing", "[shm][loan_abandon]")
{
    abandon_ring r;
    slot_subscriber consumer{r.ring, 0};
    slot_publisher pub = reliable_on(r.ring);
    loaned_buffer kept;
    {
        loaned_buffer source;
        REQUIRE(pub.loan(k_slot, source) == loan_status::ok);
        kept = std::move(source);
    }
    CHECK(take_values(consumer).empty());

    publish_loan(pub, std::move(kept), 7);
    CHECK(take_values(consumer) == std::vector<std::uint32_t>{7});
}

TEST_CASE("loan_abandon a published loan's destruction stamps nothing", "[shm][loan_abandon]")
{
    abandon_ring r;
    slot_subscriber consumer{r.ring, 0};
    slot_publisher pub = reliable_on(r.ring);
    publish_value(pub, 5);

    CHECK(take_values(consumer) == std::vector<std::uint32_t>{5});
}

TEST_CASE("loan_abandon move-assigning over an engaged loan abandons the overwritten one exactly once", "[shm][loan_abandon]")
{
    abandon_ring r;
    slot_subscriber consumer{r.ring, 0};
    slot_publisher pub = reliable_on(r.ring);
    loaned_buffer target;
    loaned_buffer other;
    REQUIRE(pub.loan(k_slot, target) == loan_status::ok);
    REQUIRE(pub.loan(k_slot, other) == loan_status::ok);
    target = std::move(other);
    publish_loan(pub, std::move(target), 1);
    REQUIRE(take_values(consumer) == std::vector<std::uint32_t>{1});

    std::vector<std::uint32_t> lap;
    for(std::uint32_t value = 2; value < 2 + k_cells; ++value)
    {
        publish_value(pub, value);
        lap.push_back(value);
    }
    CHECK(take_values(consumer) == lap);
}

TEST_CASE("loan_abandon a best-effort loan dropped without publishing leaves its cell claimable a lap later", "[shm][loan_abandon]")
{
    abandon_ring r;
    slot_publisher pub = best_effort_on(r.ring);
    {
        loaned_buffer dropped;
        REQUIRE(pub.loan(k_slot, dropped) == loan_status::ok);
    }
    for(std::uint32_t value = 1; value < k_cells; ++value)
        publish_value(pub, value);

    loaned_buffer lapping;
    CHECK(pub.loan(k_slot, lapping) == loan_status::ok);
}

TEST_CASE("loan_abandon a best-effort producer holding a loan one lap behind gives up instead of spinning", "[shm][loan_abandon]")
{
    abandon_ring r;
    slot_publisher pub = best_effort_on(r.ring);
    loaned_buffer held;
    REQUIRE(pub.loan(k_slot, held) == loan_status::ok);
    for(std::uint32_t value = 1; value < k_cells; ++value)
        publish_value(pub, value);

    loaned_buffer lapping;
    CHECK(pub.loan(k_slot, lapping) == loan_status::congested);
    publish_loan(pub, std::move(held), 0);
    CHECK(pub.loan(k_slot, lapping) == loan_status::ok);
}
