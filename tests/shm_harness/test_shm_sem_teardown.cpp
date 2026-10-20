#include "topic_regions.h"
#include "launch_helpers.h"
#include "region_doorbells.h"
#include "three_process_block.h"

#include "support/region_names.h"
#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include "plexus/shm/region_naming.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <thread>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

std::string outcome(const std::optional<std::int32_t> &status)
{
    return status ? "exit " + std::to_string(*status) : std::string("killed after the bound");
}

// The publisher creates the ring, then a waiter parks on the semaphore notifier and settles before the other process
// arms on the same ring.
std::optional<plexus::testing::xproc_process> spawn_armed(const std::string &block, const std::string &fqn, std::uint32_t index, three_role_slot &slot)
{
    const std::string_view key                         = index == k_three_publisher ? "shm_harness.three_pub" : "shm_harness.three_sub";
    std::optional<plexus::testing::xproc_process> role = plexus::testing::xproc_process::spawn(key, {block, std::to_string(index), fqn, std::string(k_posix_sem_notifier)});
    if(!role)
        return std::nullopt;
    wait_until([&] { return slot.armed.load(std::memory_order_acquire) != 0 || !role->running(); }, 10s);
    if(slot.armed.load(std::memory_order_acquire) == 0)
        return std::nullopt;
    std::this_thread::sleep_for(200ms);
    return role;
}

}

// A disarm's stop wake goes to its own waiter's semaphore, so a waiter another process parked on the ring cannot take it.
TEST_CASE("sem_teardown a waiter's disarm completes while another process waits on the same ring", "[shm][sem_teardown]")
{
    const std::string fqn = "shm_harness.sem_teardown." + std::to_string(plexus::testing::process_id());
    const topic_regions regions{fqn, {plexus::testing::test_publisher()}};
    const region_doorbells doorbells{plexus::testing::region_name(fqn, plexus::testing::test_publisher())};
    const plexus::testing::scoped_shm_name name{"sem_teardown."};
    auto block = plexus::testing::xproc_shared<three_process_block>::create(name.str());
    REQUIRE(block);

    three_role_slot &publisher                          = block->get().roles[k_three_publisher];
    three_role_slot &parked                             = block->get().roles[1];
    std::optional<plexus::testing::xproc_process> owner = spawn_armed(name.str(), fqn, k_three_publisher, publisher);
    REQUIRE(owner);
    std::optional<plexus::testing::xproc_process> waiter = spawn_armed(name.str(), fqn, 1, parked);
    REQUIRE(waiter);
    std::optional<plexus::testing::xproc_process> cycler = plexus::testing::xproc_process::spawn("shm_harness.sem_cycler", {fqn});
    REQUIRE(cycler);
    const std::optional<std::int32_t> cycled = cycler->wait_for(10s);
    cycler->kill();
    issue_command(parked, k_three_command_leave);
    const std::optional<std::int32_t> left = waiter->wait_for(10s);
    issue_command(publisher, k_three_command_leave);
    const std::optional<std::int32_t> owned = owner->wait_for(10s);
    std::cout << "sem_teardown cycler=" << outcome(cycled) << " parked=" << outcome(left) << std::endl;

    REQUIRE(cycled == std::optional<std::int32_t>(0));
    REQUIRE(left == std::optional<std::int32_t>(0));
    REQUIRE(owned == std::optional<std::int32_t>(0));
}
