#ifndef HPP_GUARD_PLEXUS_IO_RECONNECT_CONFIG_H
#define HPP_GUARD_PLEXUS_IO_RECONNECT_CONFIG_H

#include <chrono>
#include <cstdint>
#include <optional>

namespace plexus::io {

// Backoff + surrender configuration for the single-connection reconnect driver.
// min_delay/max_delay are required with NO default: a zeroed default would
// silently ship an un-tuned cadence, and the right cadence is empirically tuned
// per deployment (see the recommended_reconnect_config factory the oracle's
// virtual-clock sweep substantiates). The two surrender bounds are each
// std::optional because absence is meaningful — an absent bound does not apply.
// With neither bound set the driver retries forever at the ceiling cadence.
struct reconnect_config
{
    // The first backoff ceiling: attempt 0 jitters in [0, min_delay]. The
    // exponential growth doubles this each attempt up to max_delay.
    std::chrono::milliseconds min_delay;

    // The ceiling the exponential growth holds at: every attempt jitters in
    // [0, min(max_delay, min_delay * 2^attempt)].
    std::chrono::milliseconds max_delay;

    // Surrender after this many dial attempts (absent = never surrender on count).
    std::optional<std::uint32_t> max_attempts;

    // Surrender once this much wall/virtual time has elapsed since the first
    // attempt (absent = never surrender on elapsed time). Neither bound set ⇒
    // retry forever at the ceiling cadence.
    std::optional<std::chrono::milliseconds> max_elapsed;
};

}

#endif
