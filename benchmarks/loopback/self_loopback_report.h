#ifndef HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_REPORT_H
#define HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_REPORT_H

#include <span>
#include <array>
#include <string>
#include <vector>
#include <chrono>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <algorithm>

namespace self_loopback {

using clock_type = std::chrono::steady_clock;

inline const std::array<std::size_t, 4> g_payloads = {8, 64, 1024, 4096};

// One reduced lane cell: one-way latency tails plus the achieved throughput for one
// (carrier, payload) point, aggregated over the repeated runs of that point.
struct cell
{
    double        p50_us{};
    double        p99_us{};
    double        throughput_mps{};
    std::uint64_t delivered{};
    bool          ran{};
};

inline std::uint64_t now_count()
{
    return static_cast<std::uint64_t>(clock_type::now().time_since_epoch().count());
}

inline void write_stamp(std::span<std::byte> payload, std::uint64_t stamp)
{
    for(std::size_t i = 0; i < sizeof stamp; ++i)
        payload[i] = static_cast<std::byte>((stamp >> (8 * i)) & 0xff);
}

inline std::uint64_t read_stamp(std::span<const std::byte> bytes)
{
    std::uint64_t v = 0;
    for(std::size_t i = 0; i < sizeof v; ++i)
        v |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[i])) << (8 * i);
    return v;
}

inline double percentile(std::vector<double> &sorted, double q)
{
    if(sorted.empty())
        return 0.0;
    const auto rank = static_cast<std::size_t>(q * static_cast<double>(sorted.size() - 1));
    return sorted[rank];
}

// Reduce one point's accumulated one-way deltas and elapsed wall-time into a cell. The
// throughput is the delivered count over the captured wall-time across the point's runs.
inline cell reduce(std::vector<double> &samples_us, double wall_seconds)
{
    std::sort(samples_us.begin(), samples_us.end());
    const auto count = static_cast<std::uint64_t>(samples_us.size());
    const double mps = wall_seconds > 0.0 ? static_cast<double>(count) / wall_seconds / 1.0e6 : 0.0;
    return {percentile(samples_us, 0.50), percentile(samples_us, 0.99), mps, count, count > 0};
}

}

#endif
