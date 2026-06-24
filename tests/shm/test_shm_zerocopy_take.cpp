#include "support/handle_test_access.h"

#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/taken_message.h"

#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

// Zero-copy take: take() returns a wire_bytes<shm_slot_owner> whose view aliases
// the slot bytes; the slot's take_refcount is 1 while the owner lives and 0 after
// it destructs; the bytes stay valid (byte-equal) for the owner's whole lifetime,
// looped N>=100. No shared_ptr anywhere in the loan path.

using namespace plexus::shm;

TEST_CASE("zerocopy_take: wire_bytes aliases the slot and pins the refcount", "[shm][zerocopy_take]")
{
    constexpr int k_iterations = 200;
    for(int i = 0; i < k_iterations; ++i)
    {
        // A standin slab slot + its take_refcount (the ring would own these).
        alignas(8) std::byte slot[16]{};
        const std::uint32_t  value = 0x5A5A0000u | static_cast<std::uint32_t>(i);
        std::memcpy(slot, &value, sizeof(value));
        std::atomic<std::uint32_t> refcount{0};

        {
            // The subscriber pins the slot at take() time (refcount -> 1) and hands
            // out a move-only taken_message aliasing the slot bytes.
            taken_message msg = test::handle_test_access::make_taken(slot, sizeof(slot), &refcount);
            REQUIRE(refcount.load() == 1);

            {
                // as_wire_bytes() takes its OWN pin so the slot stays live for the
                // returned view independently of the handle.
                plexus::wire_bytes<shm_slot_owner> wb = msg.as_wire_bytes();
                REQUIRE(refcount.load() == 2);
                REQUIRE(wb.size() == sizeof(slot));
                REQUIRE(wb.data() == slot); // aliases the slot, no copy

                std::uint32_t read = 0;
                std::memcpy(&read, wb.data(), sizeof(read));
                REQUIRE(read == value);
            }
            // The wire_bytes owner destructed: its pin is released.
            REQUIRE(refcount.load() == 1);
        }
        // The taken_message destructed: refcount back to 0 (slot recyclable).
        REQUIRE(refcount.load() == 0);
    }
}

TEST_CASE("zerocopy_take: a moved wire_bytes owner unpins exactly once", "[shm][zerocopy_take]")
{
    alignas(8) std::byte       slot[8]{};
    std::atomic<std::uint32_t> refcount{0};

    {
        taken_message                      msg = test::handle_test_access::make_taken(slot, sizeof(slot), &refcount);
        plexus::wire_bytes<shm_slot_owner> a   = msg.as_wire_bytes();
        REQUIRE(refcount.load() == 2); // handle pin + view pin

        // Move the owner out: the source becomes inert, the count is unchanged.
        plexus::wire_bytes<shm_slot_owner> b = std::move(a);
        REQUIRE(refcount.load() == 2);
    }
    // Both the moved-to view AND the handle released exactly once each.
    REQUIRE(refcount.load() == 0);
}
