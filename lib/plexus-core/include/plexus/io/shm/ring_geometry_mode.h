#ifndef HPP_GUARD_PLEXUS_IO_SHM_RING_GEOMETRY_MODE_H
#define HPP_GUARD_PLEXUS_IO_SHM_RING_GEOMETRY_MODE_H

#include <cstdint>

namespace plexus::io::shm {

// How a ring's depth is derived from its declared consumer capacity and payload.
// reliable_preserving leads so it is the zero/default value (the safe-verdict-first
// convention): a caller that declares nothing gets the reliable ring.
enum class ring_geometry_mode : std::uint8_t
{
    // Size depth strictly greater than the declared consumer capacity at the
    // declared payload (pay-for-what-you-declare); the safe default. An
    // unprovisionable reliable ring fails closed, it does NOT silently downgrade.
    reliable_preserving,

    // Keep the fast SHM ring at LOW memory by admitting depth == consumers
    // (best-effort-exempt from the strict depth>consumers invariant) for large
    // payloads, trading the reliability guarantee. An EXPLICIT opt-in, NEVER reached
    // as a silent fallback from a reliable ring that cannot be provisioned.
    best_effort_large,

    // Keep a BOUNDED (capped) SHM ring for messages that fit and reroute any message
    // too large for the capped ring over the same-host wire transport. Net: reliable
    // + bounded SHM memory, trading SHM's zero-copy speed for the large messages only.
    // Its GEOMETRY here is the bounded reliable ring at the capped-payload band; the
    // per-message reroute (its only distinct value vs reliable_preserving) is built
    // separately. A clamp-only variant without the reroute would be a hollow
    // duplicate of reliable_preserving + fail-closed; the reroute is mandatory.
    wire_fallback,
};

// SHM-local provisioning hint. max_consumers 0 = unset (resolves to the shipped
// capacity floor downstream). NOT wire-advertised and NOT RxO -- producer-side
// same-host provisioning only.
struct shm_geometry
{
    std::uint32_t      max_consumers = 0;
    ring_geometry_mode mode          = ring_geometry_mode::reliable_preserving;
};

}

#endif
