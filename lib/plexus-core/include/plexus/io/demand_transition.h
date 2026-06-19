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

// Which side of a (peer, fqn) demand edge the local node plays. A SUBSCRIBER edge
// (the local node issued the subscribe — message_forwarder::attach) wants data from
// the peer, so its same-host companion ring is DRAINED into the receive path. A
// PUBLISHER edge (a remote subscribe arrived — attach_for_fanout) feeds the peer, so
// its companion ring is the SEND lane the publish fan routes fitting messages over.
// One ring (region_name_for(fqn, request)) per (peer, fqn): the publisher writes the
// producer cursor, the subscriber drains its own consumer cursor — the role decides
// which operation the local node performs on it.
enum class demand_role : std::uint8_t
{
    publisher,
    subscriber,
};

}

#endif
