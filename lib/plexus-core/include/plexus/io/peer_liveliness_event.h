#ifndef HPP_GUARD_PLEXUS_IO_PEER_LIVELINESS_EVENT_H
#define HPP_GUARD_PLEXUS_IO_PEER_LIVELINESS_EVENT_H

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

enum class liveliness_verdict : std::uint8_t
{
    alive,
    lost
};

enum class liveliness_signal : std::uint8_t
{
    none = 0,
    awareness = 1,
    heartbeat = 2,
    session = 4
};

inline liveliness_signal operator|(liveliness_signal lhs, liveliness_signal rhs)
{
    return static_cast<liveliness_signal>(
            static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
}

inline liveliness_signal operator&(liveliness_signal lhs, liveliness_signal rhs)
{
    return static_cast<liveliness_signal>(
            static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
}

// contributing carries the signal provenance of the verdict: for an alive verdict it is the
// signals currently asserting alive; for a lost verdict it is the signals consulted and found
// dead, so a consumer distinguishes a transport-drop loss from a silence loss.
struct peer_liveliness_event
{
    node_id id;
    liveliness_verdict verdict;
    liveliness_signal contributing;
};

}

#endif
