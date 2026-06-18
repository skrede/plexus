#ifndef HPP_GUARD_PLEXUS_IO_DEMAND_TRANSITION_H
#define HPP_GUARD_PLEXUS_IO_DEMAND_TRANSITION_H

#include <cstdint>

namespace plexus::io {

// The per-(peer, fqn) demand edge the message_forwarder emits and the medium
// coordinator consumes: up = the 0->1 attach (first subscribe for the pair),
// down = the 1->0 detach (last unsubscribe). Intermediate edges (1->2, 2->1)
// never emit — only the two boundary crossings the forwarder's refcount gate
// already keys the wire subscribe/unsubscribe on.
enum class demand_transition : std::uint8_t
{
    up,
    down,
};

}

#endif
