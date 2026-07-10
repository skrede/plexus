#ifndef HPP_GUARD_PLEXUS_IO_LIVELINESS_OPTIONS_H
#define HPP_GUARD_PLEXUS_IO_LIVELINESS_OPTIONS_H

#include "plexus/io/known_peers.h"

#include <chrono>
#include <cstdint>

namespace plexus::io {

enum class combine : std::uint8_t
{
    any_signal_alive,
    session_authoritative,
    all_required
};

// The node-scoped fused-liveliness knobs. awareness_ttl carries the already-swept discovery
// awareness TTL (a liveliness threshold, kept distinct from the multicast hop-limit knob).
// heartbeat_interval matches the monitor tick cadence so a heartbeat emission rides the existing
// tick without a second timer. heartbeat_interval and heartbeat_miss_limit are interim, pending
// the on-target sweep that locks the numerics.
struct liveliness_options
{
    liveliness_options()
            : awareness_ttl(std::chrono::nanoseconds(default_discovery_ttl_ns))
            , heartbeat_interval(100)
            , heartbeat_miss_limit(5)
            , policy(combine::any_signal_alive)
    {
    }

    std::chrono::nanoseconds awareness_ttl;
    std::chrono::milliseconds heartbeat_interval;
    std::uint32_t heartbeat_miss_limit;
    combine policy;
};

}

#endif
