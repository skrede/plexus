#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SEND_HISTOGRAM_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_SEND_HISTOGRAM_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace shm_harness {

// Upper bucket edges of a node's send-time histogram, in microseconds; 1000 is the publish p99 budget a probe asserts,
// and a send past the last edge lands in an overflow bucket.
inline constexpr std::array<std::uint32_t, 8> k_send_edges_us{50, 100, 250, 500, 1000, 2500, 10000, 100000};
inline constexpr std::uint32_t k_send_overflow_us = UINT32_MAX;

using send_buckets = std::array<std::atomic<std::uint32_t>, k_send_edges_us.size() + 1>;

inline void record_send(send_buckets &sends, std::uint32_t took_us)
{
    std::size_t bucket = 0;
    while(bucket < k_send_edges_us.size() && took_us > k_send_edges_us[bucket])
        ++bucket;
    sends[bucket].fetch_add(1, std::memory_order_release);
}

inline void clear_sends(send_buckets &sends)
{
    for(auto &bucket : sends)
        bucket.store(0, std::memory_order_release);
}

// The upper edge of the bucket holding the 99th-percentile send, or 0 before any send.
inline std::uint32_t p99_send_us(const send_buckets &sends)
{
    std::uint64_t total = 0;
    for(const auto &bucket : sends)
        total += bucket.load(std::memory_order_acquire);
    const std::uint64_t rank = (total * 99 + 99) / 100;
    std::uint64_t seen       = 0;
    for(std::size_t bucket = 0; total != 0 && bucket < sends.size(); ++bucket)
    {
        seen += sends[bucket].load(std::memory_order_acquire);
        if(seen >= rank)
            return bucket < k_send_edges_us.size() ? k_send_edges_us[bucket] : k_send_overflow_us;
    }
    return 0;
}

}

#endif
