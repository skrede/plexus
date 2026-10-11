#include "wake_block.h"
#include "topic_regions.h"
#include "launch_helpers.h"

#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

constexpr std::uint32_t k_trials = 500;

// One producer process and the given number of consumer processes on one ring; the first consumer creates the ring
// before the second joins and before the producer attaches.
class wake_stress
{
public:
    explicit wake_stress(std::uint32_t consumers)
            : m_fqn("shm_harness.wake." + std::to_string(plexus::testing::process_id()))
            , m_regions(m_fqn)
            , m_consumers(consumers)
            , m_name("shm_harness.wake_block.")
            , m_block(plexus::testing::xproc_shared<wake_block>::create(m_name.str()))
            , m_children()
    {
    }

    bool spawn_consumers(std::string_view leg)
    {
        for(std::uint32_t index = 0; m_block && index < m_consumers; ++index)
        {
            m_children[index] = plexus::testing::xproc_process::spawn("shm_harness.wake_consumer", {m_name.str(), std::to_string(index), m_fqn, std::string(leg)});

            const std::optional<plexus::testing::xproc_process> &child = m_children[index];
            const std::atomic<std::uint32_t> &ready                    = m_block->get().consumers[index].ready;
            if(!child || !wait_until([&] { return ready.load(std::memory_order_acquire) != 0 || !child->running(); }, 10s) || ready.load(std::memory_order_acquire) == 0)
                return false;
        }
        return m_block.has_value();
    }

    bool run_producer(std::uint32_t trials)
    {
        std::optional<plexus::testing::xproc_process> producer =
                plexus::testing::xproc_process::spawn("shm_harness.wake_producer", {m_name.str(), m_fqn, std::to_string(m_consumers), std::to_string(trials)});
        return producer && producer->wait_for(300s) == std::optional<std::int32_t>(0);
    }

    bool leave()
    {
        m_block->get().leave.store(1, std::memory_order_release);
        bool exited = true;
        for(std::uint32_t index = 0; index < m_consumers; ++index)
            exited = m_children[index] && m_children[index]->wait_for(10s) == std::optional<std::int32_t>(0) && exited;
        return exited;
    }

    wake_block &block()
    {
        return m_block->get();
    }

private:
    std::string m_fqn;
    topic_regions m_regions;
    std::uint32_t m_consumers;
    plexus::testing::scoped_shm_name m_name;
    std::optional<plexus::testing::xproc_shared<wake_block>> m_block;
    std::array<std::optional<plexus::testing::xproc_process>, k_wake_consumers> m_children;
};

void require_no_stall(std::uint32_t consumers, std::string_view leg)
{
    wake_stress stress{consumers};
    REQUIRE(stress.spawn_consumers(leg));
    const bool completed = stress.run_producer(k_trials);
    const bool left      = stress.leave();
    std::cout << "wake_stress consumers=" << consumers << " leg=" << leg << " " << wake_summary(stress.block()) << std::endl;

    REQUIRE(completed);
    REQUIRE(left);
    REQUIRE(stress.block().stalls.load(std::memory_order_acquire) == 0);
}

}

TEST_CASE("wake_stress two consumer processes observe every final message", "[.][shm-stress]")
{
    require_no_stall(2, k_production_leg);
}

TEST_CASE("wake_stress two consumer processes on the threaded leg observe every final message", "[.][shm-stress]")
{
    require_no_stall(2, k_threaded_leg);
}

TEST_CASE("wake_stress a single waiter never stalls", "[.][shm-stress]")
{
    require_no_stall(1, k_production_leg);
}
