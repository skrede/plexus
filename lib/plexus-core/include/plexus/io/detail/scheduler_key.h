#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_SCHEDULER_KEY_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_SCHEDULER_KEY_H

#include <atomic>
#include <cstdint>

namespace plexus::io::detail {

// A process-local monotonic source for the per-channel scheduler id the egress scheduler
// keys its band map on: each construction draws a fresh value so a reused heap address never
// collides with a prior (freed) channel's key — the ABA defense. A bare counter, not a
// stateful singleton: it holds no policy or lifetime, only the next id. Starts at 1 so 0 is
// reserved for the no-key short-circuit branch of the scheduler's capability probe.
[[nodiscard]] inline std::uint64_t next_scheduler_key() noexcept
{
    static std::atomic<std::uint64_t> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed) + 1;
}

}

#endif
