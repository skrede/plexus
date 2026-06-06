#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HANDLE_TEST_ACCESS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HANDLE_TEST_ACCESS_H

#include "plexus/io/shm/loaned_buffer.h"
#include "plexus/io/shm/taken_message.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

// The friend seam the move-only loan/take handles grant their tests so a unit
// case can mint a handle over a standin slot WITHOUT the slot_publisher /
// slot_subscriber endpoints (a later wave). It is the only construction path
// outside those endpoints, kept in the test tree so production code carries no
// test-only factory.

namespace plexus::io::shm::test {

struct handle_test_access
{
    static loaned_buffer make_loaned(std::byte *slot, std::size_t capacity,
                                     std::uint64_t cell_index, std::uint64_t position) noexcept
    {
        return loaned_buffer(slot, capacity, cell_index, position);
    }

    static taken_message make_taken(const std::byte *payload, std::size_t length,
                                    std::atomic<std::uint32_t> *refcount,
                                    std::uint64_t cell_index, std::uint64_t generation) noexcept
    {
        return taken_message(payload, length, refcount, cell_index, generation);
    }
};

}

#endif
