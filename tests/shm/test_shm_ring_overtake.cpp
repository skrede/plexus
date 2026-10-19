#include "test_shm_ring_core_common.h"

#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/slot_publisher.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <algorithm>

// A reliable ring with no registered cursor has no consumer gate, so only the claim protocol itself can keep a lapping
// producer off a cell whose claim one lap earlier is still uncommitted.

using namespace shm_ring_core_fixture;

namespace {

constexpr std::uint64_t k_cells = 8;
constexpr std::uint64_t k_slot  = 64;
constexpr std::byte k_held{0xa5};

struct cursorless_ring
{
    cursorless_ring()
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

void publish_one(slot_publisher &pub)
{
    loaned_buffer slot;
    REQUIRE(pub.loan(k_slot, slot) == loan_status::ok);
    std::memset(slot.bytes().data(), 0, k_slot);
    slot.set_filled(k_slot);
    REQUIRE(pub.publish(std::move(slot)) == loan_status::ok);
}

bool intact(std::span<const std::byte> held)
{
    return std::ranges::all_of(held, [](std::byte b) { return b == k_held; });
}

void lap_held_claim(slot_publisher &holder, slot_publisher &lapper)
{
    loaned_buffer held;
    REQUIRE(holder.loan(k_slot, held) == loan_status::ok);
    const std::span<const std::byte> bytes = held.bytes();
    std::memset(held.bytes().data(), std::to_integer<int>(k_held), k_slot);
    held.set_filled(k_slot);
    for(std::uint64_t i = 1; i < k_cells; ++i)
        publish_one(lapper);

    loaned_buffer lapping;
    REQUIRE(lapper.loan(k_slot, lapping) == loan_status::congested);
    REQUIRE(holder.publish(std::move(held)) == loan_status::ok);
    CHECK(intact(bytes));
    REQUIRE(lapper.loan(k_slot, lapping) == loan_status::ok);
}

}

TEST_CASE("ring_overtake a reliable claim never overwrites its own producer's claim in flight", "[shm][ring_overtake][!shouldfail]")
{
    cursorless_ring r;
    slot_publisher pub = reliable_on(r.ring);
    lap_held_claim(pub, pub);
}

TEST_CASE("ring_overtake a reliable claim never overwrites another producer's claim in flight", "[shm][ring_overtake][!shouldfail]")
{
    cursorless_ring r;
    slot_publisher predecessor = reliable_on(r.ring);
    slot_publisher successor   = reliable_on(r.ring);
    lap_held_claim(predecessor, successor);
}
