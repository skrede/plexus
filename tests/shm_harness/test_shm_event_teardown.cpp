#include "launch_helpers.h"
#include "three_process_block.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <thread>
#include <cstdint>
#include <iostream>
#include <optional>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

std::string outcome(const std::optional<std::int32_t> &status)
{
    return status ? "exit " + std::to_string(*status) : std::string("killed after the bound");
}

// The waiter parks on the event notifier and settles before the other process arms on the same ring.
std::optional<plexus::testing::xproc_process> spawn_parked(const std::string &block, const std::string &fqn, three_role_slot &slot)
{
    std::optional<plexus::testing::xproc_process> waiter = plexus::testing::xproc_process::spawn("shm_harness.three_sub", {block, "1", fqn, std::string(k_platform_notifier)});
    if(!waiter)
        return std::nullopt;
    wait_until([&] { return slot.armed.load(std::memory_order_acquire) != 0 || !waiter->running(); }, 10s);
    if(slot.armed.load(std::memory_order_acquire) == 0)
        return std::nullopt;
    std::this_thread::sleep_for(200ms);
    return waiter;
}

}

// Each waiter owns its event, so one process's disarm cancels only its own wait while the other stays parked on the ring.
TEST_CASE("event_teardown a waiter's disarm completes while another process waits on the same ring", "[shm][event_teardown]")
{
    const std::string fqn = "shm_harness.event_teardown." + std::to_string(plexus::testing::process_id());
    const plexus::testing::scoped_shm_name name{"shm_harness.event_teardown_block."};
    auto block = plexus::testing::xproc_shared<three_process_block>::create(name.str());
    REQUIRE(block);

    three_role_slot &parked                              = block->get().roles[1];
    std::optional<plexus::testing::xproc_process> waiter = spawn_parked(name.str(), fqn, parked);
    REQUIRE(waiter);
    std::optional<plexus::testing::xproc_process> cycler = plexus::testing::xproc_process::spawn("shm_harness.event_cycler", {fqn});
    REQUIRE(cycler);
    const std::optional<std::int32_t> cycled = cycler->wait_for(10s);
    cycler->kill();
    issue_command(parked, k_three_command_leave);
    const std::optional<std::int32_t> left = waiter->wait_for(10s);
    std::cout << "event_teardown cycler=" << outcome(cycled) << " parked=" << outcome(left) << std::endl;

    REQUIRE(cycled == std::optional<std::int32_t>(0));
    REQUIRE(left == std::optional<std::int32_t>(0));
}
