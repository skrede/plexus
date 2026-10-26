#ifndef HPP_GUARD_PLEXUS_RELAY_POSTURE_H
#define HPP_GUARD_PLEXUS_RELAY_POSTURE_H

#include <cstddef>

namespace plexus {

// A node's cooperative relay posture, as disclosed by self_check(). offering_relay is a compile-time
// property of the profile — a node not spelled relay<> resolves the structurally-absent emitter twin
// and never offers, so the count of origins it relays is meaningful only when offering. declining is
// a cooperative request to peers not to relay for this node, never a security boundary.
struct relay_posture
{
    bool offering_relay;
    std::size_t reported_origins;
    std::size_t routes_via_relays;
    bool declining;
};

}

#endif
