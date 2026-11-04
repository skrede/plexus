#ifndef HPP_GUARD_PLEXUS_IO_DISCONNECT_CAUSE_H
#define HPP_GUARD_PLEXUS_IO_DISCONNECT_CAUSE_H

#include <cstdint>

namespace plexus::io {

// Append-only: observers may persist or switch on these values, so new causes extend the tail.
enum class disconnect_cause : std::uint8_t
{
    unspecified,
    heartbeat_timeout
};

}

#endif
