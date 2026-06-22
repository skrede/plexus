#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_SELECTION_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_SELECTION_H

#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/shm/same_host.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io::shm {

// The medium a same-host (peer, topic) pair resolves to. shm = the pair is
// same-host AND carries a qualifying hint, so each side attempts the ring acquire.
// stream = the pair stays on the local stream (AF_UNIX) — the fallback. The wire
// attach is NEVER suppressed regardless of this verdict (dual-delivery): a
// shared-memory upgrade ADDS a same-host fast path, it does not REPLACE the wire
// path, so a peer that cannot map the ring (or a third process that never upgrades)
// still receives over the stream. This enum only records which fast path, if any,
// the pair additionally attempts.
enum class same_host_medium : std::uint8_t
{
    stream,
    shm,
};

// The pure selection decision: a (peer, topic) pair is
// shared-memory eligible iff the peer is same-host AND either side declared a
// qualifying dispatch hint. The hint is consumer-sovereign and bilateral — it is
// the OR of whatever this side declared with whatever the peer declared (passed in
// already combined by the caller); EITHER end's hint upgrades BOTH ends, and both
// independently converge on region_name_for(fqn) with NO wire exchange. The hint
// only gates whether each side ATTEMPTS the acquire; the upgrade_coordinator mints
// the companion ring through the shm member (the broker-failure fallback keeps the wire).
[[nodiscard]] inline same_host_medium select_same_host_medium(host_fingerprint peer,
                                                              host_fingerprint local,
                                                              dispatch_hint combined_hint) noexcept
{
    if(is_same_host(peer, local) && shm_eligible(combined_hint))
        return same_host_medium::shm;
    return same_host_medium::stream;
}

// The ONLY per-message medium decision in plexus, and the sole distinct value of
// wire_fallback: every other selection (select_same_host_medium, attempt_shm_upgrade)
// is per-(peer, topic) and decided ONCE at dial time on host-distinctness + hint.
// wire_fallback keeps a BOUNDED (capped) reliable ring for messages that fit and
// reroutes any message larger than the cap over the same-host wire — so a same-host
// peer in this mode needs a per-message size check the other modes never run.
//
// It is a no-op (always shm) for reliable_preserving and best_effort_large: their
// per-peer verdict already chose the medium once, so this returns shm and the caller's
// gate (mode == wire_fallback) keeps the branch out of their path entirely. Only
// wire_fallback AND message_bytes > the capped slot capacity routes to the wire
// (stream); a wire_fallback message that fits the cap rides the ring. Pure: it reads
// the LOCAL message size against the LOCALLY-resolved cap, never anything wire-supplied.
[[nodiscard]] inline same_host_medium
route_message_medium(ring_geometry_mode mode, std::size_t message_bytes,
                     std::uint64_t capped_slot_capacity) noexcept
{
    if(mode != ring_geometry_mode::wire_fallback)
        return same_host_medium::shm;
    return message_bytes <= capped_slot_capacity ? same_host_medium::shm : same_host_medium::stream;
}

// The bilateral, consumer-sovereign upgrade decision: whether THIS end
// attempts the same-host shared-memory acquire for a (peer, topic). It is purely
// LOCAL — it reads only this end's same-host verdict (recorded from the handshake
// fingerprint) and this end's own dispatch hint — and exchanges NOTHING on the wire.
// Both ends run this independently and, when it returns true, each acquires the SAME
// deterministically-named ring (region_name_for(fqn, direction)); they converge on
// the one ring the same way they converge on the name (demand-driven convergence).
// The hint ONLY gates whether each side ATTEMPTS the acquire:
//   * a PUBLISHER with a qualifying hint creates + sizes the ring (max_payload);
//   * a SUBSCRIBER with a qualifying hint attaches the same ring (creating the
//     default-geometry ring itself if the publisher has not yet) -- the consumer
//     rescues itself from a hint-less publisher (the beyond-vagus capability).
// EITHER end's qualifying hint therefore upgrades the pair: the side with the hint
// attempts the acquire, and the converged ring is shared. A non-same-host pair never
// upgrades regardless of the hint (the eligibility gate).
[[nodiscard]] inline bool attempt_shm_upgrade(bool same_host, dispatch_hint own_hint) noexcept
{
    return same_host && shm_eligible(own_hint);
}

// The ring-sizing authority for an upgrade THIS end drives: a publisher with
// a declared max_payload sizes the ring to that width; a subscriber-only upgrade (no
// max_payload, i.e. 0) falls back to the default ring geometry. Returns the value to
// pass to shm_topic_registry::acquire, where 0 already means "default geometry".
[[nodiscard]] inline std::uint32_t
upgrade_ring_max_payload(ring_direction direction, std::uint32_t publisher_max_payload) noexcept
{
    return direction == ring_direction::request ? publisher_max_payload : 0u;
}

}

#endif
