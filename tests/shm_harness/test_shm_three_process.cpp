#include "launch_helpers.h"
#include "three_process_block.h"

#if !defined(_WIN32)
    #include "topic_regions.h"
    #include "region_doorbells.h"
#endif

#include "support/region_names.h"
#include "support/xproc_control.h"
#include "support/xproc_harness.h"
#include "support/xproc_process.h"

#include "plexus/shm/region_naming.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <cstdint>
#include <utility>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

constexpr std::uint32_t k_rounds = 10;
constexpr std::array<std::uint32_t, 2> k_subscribers{1, 2};

// One publisher process and two subscriber processes, each with its own registry and notifier on one ring. Every
// waiter is parked before a round's single publish, so a wake that reaches only one of them shows as a missed round.
class three_process
{
public:
    three_process(std::string fqn, std::string_view notifier)
            : m_fqn(std::move(fqn))
            , m_notifier(notifier)
            , m_name("shm_harness.three.")
            , m_block(plexus::testing::xproc_shared<three_process_block>::create(m_name.str()))
            , m_children()
    {
    }

    bool start()
    {
        if(!m_block)
        {
            std::cout << "three_process start: the shared block was not created" << std::endl;
            return false;
        }
        if(!spawn(k_three_publisher) || !armed(k_three_publisher))
            return report_refusal(k_three_publisher);
        for(const std::uint32_t index : k_subscribers)
            if(!spawn(index))
                return report_refusal(index);
        for(const std::uint32_t index : k_subscribers)
            if(!armed(index))
                return report_refusal(index);
        std::this_thread::sleep_for(200ms);
        return true;
    }

    // Returns each subscriber's latency in microseconds, or nothing for a subscriber the publish did not reach.
    std::array<std::optional<std::uint32_t>, 2> publish_round()
    {
        const std::array<std::uint32_t, 2> before{delivered(1), delivered(2)};
        three_role_slot &publisher = role(k_three_publisher);
        issue_command(publisher, k_three_command_publish);
        wait_until([&] { return delivered(1) > before[0] && delivered(2) > before[1]; }, 2s);
        wait_until([&] { return acknowledged(publisher); }, 2s);
        return {reached(1, before[0]), reached(2, before[1])};
    }

    bool leave()
    {
        for(std::uint32_t index = 0; index < k_three_roles; ++index)
            issue_command(role(index), k_three_command_leave);
        bool exited = true;
        for(std::optional<plexus::testing::xproc_process> &child : m_children)
            exited = child && child->wait_for(10s) == std::optional<std::int32_t>(0) && exited;
        return exited;
    }

private:
    std::string m_fqn;
    std::string m_notifier;
    plexus::testing::scoped_shm_name m_name;
    std::optional<plexus::testing::xproc_shared<three_process_block>> m_block;
    std::array<std::optional<plexus::testing::xproc_process>, k_three_roles> m_children;

    bool spawn(std::uint32_t index)
    {
        const std::string_view key = index == k_three_publisher ? "shm_harness.three_pub" : "shm_harness.three_sub";
        m_children[index]          = plexus::testing::xproc_process::spawn(key, {m_name.str(), std::to_string(index), m_fqn, m_notifier});
        return m_children[index].has_value();
    }

    bool report_refusal(std::uint32_t index)
    {
        std::optional<plexus::testing::xproc_process> &child = m_children[index];
        std::cout << "three_process start: role " << index << (child ? " did not arm" : " did not spawn");
        if(child)
        {
            const std::optional<std::int32_t> status = child->wait_for(0ms);
            std::cout << (status ? ", exit status " + std::to_string(*status) : std::string(", still running"));
        }
        std::cout << std::endl;
        return false;
    }

    bool armed(std::uint32_t index)
    {
        const std::optional<plexus::testing::xproc_process> &child = m_children[index];
        const std::atomic<std::uint32_t> &flag                     = role(index).armed;
        wait_until([&] { return flag.load(std::memory_order_acquire) != 0 || !child->running(); }, 10s);
        return flag.load(std::memory_order_acquire) != 0;
    }

    std::optional<std::uint32_t> reached(std::uint32_t index, std::uint32_t before)
    {
        if(delivered(index) <= before)
            return std::nullopt;
        return role(index).last_latency_us.load(std::memory_order_acquire);
    }

    std::uint32_t delivered(std::uint32_t index)
    {
        return role(index).delivered.load(std::memory_order_acquire);
    }

    three_role_slot &role(std::uint32_t index)
    {
        return m_block->get().roles[index];
    }
};

bool report_round(std::uint32_t round, const std::array<std::optional<std::uint32_t>, 2> &latencies)
{
    std::cout << "three_process round=" << round;
    for(std::uint32_t sub = 0; sub < latencies.size(); ++sub)
        std::cout << " sub" << sub << "=" << (latencies[sub] ? std::to_string(*latencies[sub]) + "us" : std::string("missed"));
    std::cout << std::endl;
    return latencies[0] && latencies[1];
}

void require_every_round(std::string_view notifier)
{
    const std::string fqn = "shm_harness.three." + std::to_string(plexus::testing::process_id());
#if !defined(_WIN32)
    const topic_regions regions{fqn, {plexus::testing::test_publisher()}};
    const region_doorbells doorbells{plexus::testing::region_name(fqn, plexus::testing::test_publisher())};
#endif
    three_process topology{fqn, notifier};
    REQUIRE(topology.start());

    std::uint32_t reached = 0;
    for(std::uint32_t round = 0; round < k_rounds; ++round)
        reached += report_round(round, topology.publish_round()) ? 1 : 0;
    const bool left = topology.leave();
    std::cout << "three_process notifier=" << notifier << " reached=" << reached << "/" << k_rounds << " left=" << left << std::endl;

    REQUIRE(reached == k_rounds);
    REQUIRE(left);
}

}

#if defined(__linux__)
TEST_CASE("three_process one publish reaches both subscriber processes", "[shm][three_process]")
{
    require_every_round(k_platform_notifier);
}

TEST_CASE("three_process one publish reaches both subscriber processes through POSIX semaphores", "[shm][three_process]")
{
    require_every_round(k_posix_sem_notifier);
}
#else
TEST_CASE("three_process one publish reaches both subscriber processes", "[shm][three_process]")
{
    require_every_round(k_platform_notifier);
}
#endif
