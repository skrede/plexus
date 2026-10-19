#include "region_doorbells.h"

#include "support/region_names.h"

#include "plexus/asio/shm/macos/sem_doorbell.h"

#include "plexus/shm/region_naming.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>
#include <optional>

#include <fcntl.h>
#include <semaphore.h>

namespace {

constexpr std::uint32_t k_slot      = 3;
constexpr std::uint32_t k_first_tag = 41;

// macOS does not implement sem_getvalue, so a count is read by draining it.
std::int32_t drain_count(sem_t *sem)
{
    std::int32_t count = 0;
    while(::sem_trywait(sem) == 0)
        ++count;
    return count;
}

std::int32_t count_by_name(const char *name)
{
    sem_t *sem = ::sem_open(name, 0);
    if(sem == SEM_FAILED)
        return -1;
    const std::int32_t count = drain_count(sem);
    ::sem_close(sem);
    return count;
}

}

TEST_CASE("sem_adoption a pre-existing semaphore with a residual count is never adopted", "[shm][sem_adoption]")
{
    const std::string region = plexus::testing::region_name("shm_harness.sem_adoption." + std::to_string(plexus::testing::process_id()), plexus::testing::test_publisher());
    const shm_harness::region_doorbells doorbells{region};
    const std::string taken = shm_harness::sem_name_for(region, k_slot, k_first_tag);
    sem_t *stale            = ::sem_open(taken.c_str(), O_CREAT | O_EXCL, 0600, 3);
    REQUIRE(stale != SEM_FAILED);

    std::uint32_t next_tag = k_first_tag;
    plexus::asio::shm::detail::sem_doorbell doorbell;
    const std::optional<std::uint32_t> tag = doorbell.create(region, k_slot, [&next_tag] { return next_tag++; });
    const std::int32_t stale_count         = drain_count(stale);
    ::sem_close(stale);

    REQUIRE(tag == std::optional<std::uint32_t>(k_first_tag + 1));
    REQUIRE(doorbell.name().view() != taken);
    REQUIRE(stale_count == 3);
    REQUIRE(count_by_name(doorbell.name().c_str()) == 0);
}
