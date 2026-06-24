#ifndef HPP_GUARD_PLEXUS_IO_RECONNECT_CONFIG_H
#define HPP_GUARD_PLEXUS_IO_RECONNECT_CONFIG_H

#include <chrono>
#include <cstdint>
#include <optional>

namespace plexus::io {

// Each attempt jitters in [0, min(max_delay, min_delay * 2^attempt)]. An absent
// surrender bound does not apply; with neither set the driver retries forever.
struct reconnect_config
{
    std::chrono::milliseconds min_delay;
    std::chrono::milliseconds max_delay;
    std::optional<std::uint32_t> max_attempts;
    std::optional<std::chrono::milliseconds> max_elapsed;
};

}

#endif
