#include "sem_support.h"

#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include "plexus/shm/region_naming.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <cstdint>
#include <iostream>
#include <optional>

using namespace std::chrono_literals;

// A burst larger than the wake pipe's capacity blocks the waiter on its write while the reactor is not reading.
TEST_CASE("sem_pipe a disarm completes after a burst of signals while the reactor is stalled", "[shm][sem_pipe][!shouldfail]")
{
    const std::string fqn = "shm_harness.sem_pipe." + std::to_string(plexus::testing::process_id());
    const shm_harness::scoped_sem_name sem{plexus::shm::region_name_for(fqn, plexus::shm::ring_direction::request)};

    std::optional<plexus::testing::xproc_process> flood = plexus::testing::xproc_process::spawn("shm_harness.sem_flood", {fqn});
    REQUIRE(flood);
    const std::optional<std::int32_t> exited = flood->wait_for(10s);
    flood->kill();
    std::cout << "sem_pipe flood=" << (exited ? "exit " + std::to_string(*exited) : std::string("killed after the bound")) << std::endl;

    REQUIRE(exited == std::optional<std::int32_t>(0));
}
