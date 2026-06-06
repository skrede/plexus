#include "support/handle_test_access.h"

#include "plexus/io/shm/loaned_buffer.h"
#include "plexus/io/shm/shm_slot_owner.h"
#include "plexus/io/shm/taken_message.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

// Move-only loan lifetime: a moved-from loaned_buffer / taken_message / owner is
// inert (no double-commit, no double-unpin). The handles are already plexus
// single-owner discipline; this proves the steal+null + idempotent-reclaim shape.

using namespace plexus::io::shm;

TEST_CASE("loan_lifetime: a moved-from loaned_buffer is inert", "[shm][loan_lifetime]")
{
    alignas(8) std::byte slot[32]{};

    loaned_buffer a = test::handle_test_access::make_loaned(slot, sizeof(slot), 0, 0);
    REQUIRE(a.capacity() == sizeof(slot));
    REQUIRE(a.bytes().data() == slot);

    a.set_filled(16);
    REQUIRE(a.filled() == 16);

    // Move steals the window and nulls the source.
    loaned_buffer b = std::move(a);
    REQUIRE(b.capacity() == sizeof(slot));
    REQUIRE(b.filled() == 16);
    REQUIRE(a.capacity() == 0); // the moved-from source is inert
    REQUIRE(a.filled() == 0);

    // Destroying both (b then a) must not double-anything — reclaim is idempotent.
    // (No observable side effect at this layer beyond not crashing / not asserting.)
}

TEST_CASE("loan_lifetime: a moved-from taken_message unpins exactly once", "[shm][loan_lifetime]")
{
    alignas(8) std::byte slot[8]{};
    std::atomic<std::uint32_t> refcount{0};

    {
        taken_message a = test::handle_test_access::make_taken(slot, sizeof(slot), &refcount, 0, 0);
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
