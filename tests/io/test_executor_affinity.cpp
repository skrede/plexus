#include "plexus/detail/executor_affinity.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <latch>
#include <thread>
#include <cstddef>
#include <cstdint>

namespace {

using affinity = plexus::detail::executor_affinity<std::uint32_t>;

constexpr std::uint32_t k_setup  = 1;
constexpr std::uint32_t k_runner = 2;
constexpr std::uint32_t k_other  = 3;

constexpr bool k_outside = false;
constexpr bool k_inside  = true;

std::array<bool, 2> race_first_observations(bool inside_handler)
{
    plexus::detail::executor_affinity<std::thread::id> record;
    std::array<bool, 2> driver{};
    std::latch start{2};
    auto observer = [&](std::size_t slot)
    {
        start.arrive_and_wait();
        driver[slot] = record.observe(std::this_thread::get_id(), inside_handler);
    };
    std::thread first{observer, 0};
    std::thread second{observer, 1};
    first.join();
    second.join();
    return driver;
}

}

TEST_CASE("executor_affinity binds the first observer outside a handler", "[io][executor_affinity]")
{
    affinity record;
    CHECK(record.observe(k_setup, k_outside));
    CHECK_FALSE(record.observe(k_other, k_outside));
    CHECK(record.observe(k_setup, k_outside));
}

TEST_CASE("executor_affinity hands over from the setup thread to the first in-handler observer", "[io][executor_affinity]")
{
    affinity record;
    REQUIRE(record.observe(k_setup, k_outside));
    CHECK(record.observe(k_runner, k_inside));
    CHECK_FALSE(record.observe(k_setup, k_outside));
    CHECK_FALSE(record.observe(k_setup, k_inside));
}

TEST_CASE("executor_affinity confirms an in-handler observer on an unbound record", "[io][executor_affinity]")
{
    affinity record;
    CHECK(record.observe(k_runner, k_inside));
    CHECK_FALSE(record.observe(k_setup, k_outside));
}

TEST_CASE("executor_affinity rejects a second concurrent runner inside a handler", "[io][executor_affinity]")
{
    affinity record;
    REQUIRE(record.observe(k_runner, k_inside));
    CHECK_FALSE(record.observe(k_other, k_inside));
    CHECK(record.observe(k_runner, k_inside));
}

TEST_CASE("executor_affinity reports the confirmed driver outside a handler as the driver", "[io][executor_affinity]")
{
    affinity record;
    REQUIRE(record.observe(k_setup, k_outside));
    REQUIRE(record.observe(k_runner, k_inside));
    CHECK(record.observe(k_runner, k_outside));
}

TEST_CASE("executor_affinity leaves a first-observing worker bound until a handler hands over", "[io][executor_affinity]")
{
    constexpr std::uint32_t worker = 9;
    affinity record;
    CHECK(record.observe(worker, k_outside));
    CHECK(record.observe(worker, k_outside));
    CHECK_FALSE(record.observe(k_setup, k_outside));
    CHECK(record.observe(k_runner, k_inside));
    CHECK_FALSE(record.observe(worker, k_outside));
}

TEST_CASE("executor_affinity release returns a confirmed record to unbound", "[io][executor_affinity]")
{
    affinity record;
    REQUIRE(record.observe(k_runner, k_inside));
    record.release();
    CHECK(record.observe(k_other, k_outside));
    CHECK_FALSE(record.observe(k_runner, k_outside));
}

TEST_CASE("executor_affinity binds exactly one of two racing first observers", "[io][executor_affinity]")
{
    constexpr std::uint32_t trials = 500;
    for(std::uint32_t trial = 0; trial < trials; ++trial)
    {
        const std::array<bool, 2> driver = race_first_observations(k_outside);
        REQUIRE(driver[0] != driver[1]);
    }
}

TEST_CASE("executor_affinity confirms exactly one of two racing in-handler observers", "[io][executor_affinity]")
{
    constexpr std::uint32_t trials = 500;
    for(std::uint32_t trial = 0; trial < trials; ++trial)
    {
        const std::array<bool, 2> driver = race_first_observations(k_inside);
        REQUIRE(driver[0] != driver[1]);
    }
}
