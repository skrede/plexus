#ifndef HPP_GUARD_PLEXUS_SHM_SHM_SELECTION_H
#define HPP_GUARD_PLEXUS_SHM_SHM_SELECTION_H

#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/region_naming.h"
#include "plexus/io/dispatch_hint.h"
#include "plexus/io/host_fingerprint.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::shm {

// Which fast path, if any, a same-host (peer, topic) pair additionally attempts. The
// wire attach is NEVER suppressed regardless of this verdict (dual-delivery): a
// shared-memory upgrade ADDS a same-host fast path, it does not REPLACE the wire path.
enum class same_host_medium : std::uint8_t
{
    stream,
    shm,
};

// shared-memory eligible iff the peer is same-host AND either side declared a qualifying
// dispatch hint (the hint is the OR of both ends, combined by the caller). Both ends
// independently converge on region_name_for(fqn) with NO wire exchange.
inline same_host_medium select_same_host_medium(io::host_fingerprint peer, io::host_fingerprint local, io::dispatch_hint combined_hint) noexcept
{
    if(io::is_same_host(peer, local) && io::any_set(combined_hint))
        return same_host_medium::shm;
    return same_host_medium::stream;
}

// The ONLY per-message medium decision and the sole distinct value of wire_fallback
// (the other selections are per-(peer, topic), decided once at dial time). A no-op
// (always shm) for the other modes; only wire_fallback AND message_bytes past the
// capped slot capacity routes to the wire. Reads the LOCAL message size against the
// LOCALLY-resolved cap, never anything wire-supplied.
inline same_host_medium route_message_medium(ring_geometry_mode mode, std::size_t message_bytes, std::uint64_t capped_slot_capacity) noexcept
{
    if(mode != ring_geometry_mode::wire_fallback)
        return same_host_medium::shm;
    return message_bytes <= capped_slot_capacity ? same_host_medium::shm : same_host_medium::stream;
}

// Whether THIS end attempts the same-host shared-memory acquire for a (peer, topic).
// Purely LOCAL (this end's same-host verdict + its own dispatch hint), exchanges
// nothing on the wire. Both ends run it independently and, when true, each acquires the
// SAME deterministically-named ring. EITHER end's qualifying hint upgrades the pair; a
// non-same-host pair never upgrades regardless of the hint.
inline bool attempt_shm_upgrade(bool same_host, io::dispatch_hint own_hint) noexcept
{
    return same_host && io::any_set(own_hint);
}

// A publisher (request direction) sizes the ring to its declared max_payload; a
// subscriber-only upgrade passes 0, which acquire() reads as the default geometry.
inline std::uint32_t upgrade_ring_max_payload(ring_direction direction, std::uint32_t publisher_max_payload) noexcept
{
    return direction == ring_direction::request ? publisher_max_payload : 0u;
}

}

#endif
