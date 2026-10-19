#include "test_shm_ring_core_common.h"

#include "support/handle_test_access.h"

#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/taken_message.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/slot_publisher.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

// Move-only loan lifetime: a moved-from loaned_buffer / taken_message / owner is
// inert (no double-commit, no double-unpin). The handles are already plexus
// single-owner discipline; this proves the steal+null + idempotent-reclaim shape.

using namespace shm_ring_core_fixture;

TEST_CASE("loan_lifetime: a moved-from loaned_buffer is inert", "[shm][loan_lifetime]")
{
    backing_region control{control_region_bytes(4)};
    backing_region slab{slab_region_bytes(4, 32)};
    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), 4, 32, plexus::node_id{}, ring) == loan_status::ok);
    slot_publisher pub{ring, plexus::io::reliability::reliable, plexus::io::congestion::block};

    loaned_buffer a;
    REQUIRE(pub.loan(32, a) == loan_status::ok);
    const std::span<std::byte> window = a.bytes();
    a.set_filled(16);
    REQUIRE(a.filled() == 16);

    loaned_buffer b = std::move(a);
    REQUIRE(b.bytes().data() == window.data());
    REQUIRE(b.capacity() == window.size());
    REQUIRE(b.filled() == 16);
    REQUIRE(a.capacity() == 0);
    REQUIRE(a.filled() == 0);
}

TEST_CASE("loan_lifetime: a moved-from taken_message unpins exactly once", "[shm][loan_lifetime]")
{
    alignas(8) std::byte slot[8]{};
    std::atomic<std::uint32_t> refcount{0};

    {
        taken_message a = test::handle_test_access::make_taken(slot, sizeof(slot), &refcount);
        REQUIRE(refcount.load() == 1);

        taken_message b = std::move(a);
        REQUIRE(refcount.load() == 1); // the move did not double-pin or unpin

        // Move-assign onto a live target: the target's prior pin (none here) is
        // released first, then the source is stolen and nulled.
        taken_message c;
        c = std::move(b);
        REQUIRE(refcount.load() == 1);
    }
    // Exactly one unpin across all the moves: the final live handle released once.
    REQUIRE(refcount.load() == 0);
}

TEST_CASE("loan_lifetime: shm_slot_owner pins on construct and unpins once", "[shm][loan_lifetime]")
{
    std::atomic<std::uint32_t> refcount{0};

    {
        shm_slot_owner owner{&refcount};
        REQUIRE(refcount.load() == 1);

        shm_slot_owner moved = std::move(owner);
        REQUIRE(refcount.load() == 1); // move steals, does not re-pin

        // A default-constructed (null) owner pins nothing and unpins nothing.
        shm_slot_owner empty;
        REQUIRE(refcount.load() == 1);
    }
    REQUIRE(refcount.load() == 0);
}
