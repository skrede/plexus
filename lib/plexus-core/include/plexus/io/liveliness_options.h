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
// tick without a second timer. heartbeat_interval and heartbeat_miss_limit are validated against an
// on-target detection-latency sweep. session_teardown_timeout bounds how long a complete session may stay
// silent before it is torn down; it is kept apart from the miss window so a short pause (a debugger
// breakpoint) keeps the session while the heartbeat verdict reads lost. Zero disables the teardown.
struct liveliness_options
{
    liveliness_options()
            : awareness_ttl(std::chrono::nanoseconds(default_discovery_ttl_ns))
            , heartbeat_interval(100)
            , heartbeat_miss_limit(5)
            , session_teardown_timeout(std::chrono::seconds(10))
            , policy(combine::any_signal_alive)
    {
    }

    std::chrono::nanoseconds awareness_ttl;
    std::chrono::milliseconds heartbeat_interval;
    std::uint32_t heartbeat_miss_limit;
    std::chrono::nanoseconds session_teardown_timeout;
    combine policy;
};

}

#endif
