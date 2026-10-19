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

void publish_value(slot_publisher &pub, std::uint32_t value)
{
    loaned_buffer slot;
    REQUIRE(pub.loan(sizeof(value), slot) == loan_status::ok);
    std::memcpy(slot.bytes().data(), &value, sizeof(value));
    slot.set_filled(sizeof(value));
    REQUIRE(pub.publish(std::move(slot)) == loan_status::ok);
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

TEST_CASE("loan_abandon a reliable loan dropped without publishing does not stop later publishes reaching a consumer", "[shm][loan_abandon][!shouldfail]")
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
