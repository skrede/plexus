#ifndef HPP_GUARD_PLEXUS_SHM_RING_GEOMETRY_MODE_H
#define HPP_GUARD_PLEXUS_SHM_RING_GEOMETRY_MODE_H

#include <cstdint>

namespace plexus::shm {

// reliable_preserving leads so it is the zero/default (the safe-verdict-first
// convention): a caller that declares nothing gets the reliable ring.
enum class ring_geometry_mode : std::uint8_t
{
    // Depth strictly greater than the declared consumer capacity; fails closed
    // when unprovisionable, it does NOT silently downgrade.
    reliable_preserving,

    // Admits depth == consumers (exempt from the strict depth>consumers invariant)
    // for large payloads, trading the reliability guarantee. An EXPLICIT opt-in,
    // NEVER reached as a silent fallback from a reliable ring.
    best_effort_large,

    // A bounded (capped) reliable ring for messages that fit; any message too large
    // for the cap reroutes over the same-host wire. Its geometry here is the bounded
    // reliable ring at the capped-payload band; the per-message reroute is built
    // separately.
    wire_fallback,
};

// SHM-local provisioning hint. max_consumers 0 = unset (resolves to the shipped
// capacity floor downstream). Producer-side same-host provisioning only.
struct shm_geometry
{
    std::uint32_t max_consumers = 0;
    ring_geometry_mode mode     = ring_geometry_mode::reliable_preserving;
};

}

#endif
