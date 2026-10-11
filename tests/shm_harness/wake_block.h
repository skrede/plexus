#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_WAKE_BLOCK_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_WAKE_BLOCK_H

#include <array>
#include <atomic>
#include <string>
#include <cstdint>
#include <string_view>

namespace shm_harness {

inline constexpr std::uint32_t k_wake_consumers    = 2;
inline constexpr std::string_view k_threaded_leg   = "threaded";
inline constexpr std::string_view k_production_leg = "production";

struct wake_consumer_slot
{
    std::atomic<std::uint32_t> ready;
    std::atomic<std::uint32_t> received;
};

struct wake_block
{
    std::array<wake_consumer_slot, k_wake_consumers> consumers;
    std::atomic<std::uint32_t> leave;
    std::atomic<std::uint32_t> trials;
    std::atomic<std::uint32_t> stalls;
    std::atomic<std::uint32_t> p50_us;
    std::atomic<std::uint32_t> p99_us;
    std::atomic<std::uint32_t> max_us;
};

inline std::string wake_summary(const wake_block &block)
{
    return "trials=" + std::to_string(block.trials.load(std::memory_order_acquire)) + " stalls=" + std::to_string(block.stalls.load(std::memory_order_acquire)) +
            " p50_us=" + std::to_string(block.p50_us.load(std::memory_order_acquire)) + " p99_us=" + std::to_string(block.p99_us.load(std::memory_order_acquire)) +
            " max_us=" + std::to_string(block.max_us.load(std::memory_order_acquire));
}

}

#endif
