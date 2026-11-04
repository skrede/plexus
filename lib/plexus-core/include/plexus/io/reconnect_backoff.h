#ifndef HPP_GUARD_PLEXUS_IO_RECONNECT_BACKOFF_H
#define HPP_GUARD_PLEXUS_IO_RECONNECT_BACKOFF_H

#include "plexus/io/reconnect_config.h"

#include <chrono>
#include <random>
#include <cstdint>
#include <algorithm>

namespace plexus::io {

// Full-jitter exponential backoff: delay = uniform(0, min(max_delay, min_delay * 2^attempt)).
// The shift is capped at 20 so the 64-bit scale cannot overflow.
template<typename Rng>
std::chrono::milliseconds compute_backoff(const reconnect_config &cfg, std::uint32_t attempt, Rng &rng)
{
    const auto shift = std::min(attempt, std::uint32_t{20});
    auto ceiling     = cfg.min_delay * (std::uint64_t{1} << shift);
    if(ceiling > cfg.max_delay)
        ceiling = cfg.max_delay;
    std::uniform_int_distribution<std::int64_t> dist{0, static_cast<std::int64_t>(ceiling.count())};
    return std::chrono::milliseconds{dist(rng)};
}

inline bool surrender_reached(const reconnect_config &cfg, std::uint32_t attempt, std::chrono::milliseconds elapsed) noexcept
{
    return (cfg.max_attempts.has_value() && attempt >= *cfg.max_attempts) || (cfg.max_elapsed.has_value() && elapsed >= *cfg.max_elapsed);
}

}

#endif
