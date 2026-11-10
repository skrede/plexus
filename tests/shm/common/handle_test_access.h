#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HANDLE_TEST_ACCESS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HANDLE_TEST_ACCESS_H

#include "plexus/shm/taken_message.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

// The friend seam taken_message grants its tests, so a unit case can pin a standin slot without a slot_subscriber
// while production code carries no test-only factory.

namespace plexus::shm::test {

struct handle_test_access
{
    static taken_message make_taken(const std::byte *payload, std::size_t length, std::atomic<std::uint32_t> *refcount) noexcept
    {
        return taken_message(payload, length, refcount);
    }
};

}

#endif
