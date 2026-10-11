#include "sem_support.h"

#include "support/heap_ring.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/region_naming.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <cstdint>
#include <iostream>

#include <fcntl.h>
#include <semaphore.h>

using namespace std::chrono_literals;

namespace {

// Arms a semaphore notifier on the region and reads the pre-created semaphore after its waiter has had time to run.
std::int32_t value_after_arm(plexus::testing::heap_ring &heap, const std::string &region, sem_t *stale)
{
    ::asio::io_context io;
    shm_harness::sem_notifier notifier{io, heap.ring().header(), region};
    const plexus::shm::arm_status armed = notifier.arm(0, [] {});
    io.run_for(300ms);
    int value = -1;
    ::sem_getvalue(stale, &value);
    std::cout << "sem_adoption armed=" << static_cast<std::uint32_t>(armed) << " value=" << value << std::endl;
    return value;
}

}

TEST_CASE("sem_adoption a pre-existing semaphore with a residual count is never adopted", "[shm][sem_adoption][!shouldfail]")
{
    plexus::testing::heap_ring heap{16, 64};
    REQUIRE(heap.created());
    const std::string region = plexus::shm::region_name_for("shm_harness.sem_adoption." + std::to_string(plexus::testing::process_id()), plexus::shm::ring_direction::request);
    const shm_harness::scoped_sem_name name{region};

    sem_t *stale = ::sem_open(name.str().c_str(), O_CREAT | O_EXCL, 0600, 3);
    REQUIRE(stale != SEM_FAILED);
    const std::int32_t value = value_after_arm(heap, region, stale);
    ::sem_close(stale);

    REQUIRE(value == 3);
}
