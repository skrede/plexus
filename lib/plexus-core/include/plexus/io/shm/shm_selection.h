#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_SELECTION_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_SELECTION_H

#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/detail/keyed_refcount.h"

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
// only gates whether each side ATTEMPTS the acquire; the actual ring acquire (and
// the broker-failure fallback) is the registry's job in a later wave.
[[nodiscard]] inline same_host_medium select_same_host_medium(host_fingerprint peer,
                                                              host_fingerprint local,
                                                              dispatch_hint combined_hint) noexcept
{
    if(is_same_host(peer, local) && shm_eligible(combined_hint))
        return same_host_medium::shm;
    return same_host_medium::stream;
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
[[nodiscard]] inline std::uint32_t upgrade_ring_max_payload(ring_direction direction,
                                                            std::uint32_t publisher_max_payload) noexcept
{
    return direction == ring_direction::request ? publisher_max_payload : 0u;
}

// The per-forwarder acquired-ring bookkeeping: the set of (node_name, fqn) pairs
// this forwarder currently holds a same-host ring for, plus the per-pair refcount
// that gates the 0->1 acquire and the 1->0 release. This is borrowed BY REFERENCE
// by the selection caller (no singleton — each forwarder owns its own set, the
// vagus discipline), and it keys on the plexus (node_name, fqn) identity mirroring
// subscriber_registry. It holds only the bookkeeping; the registry.acquire call
// that maps the actual ring wires in later.
class acquired_ring_set
{
public:
    static constexpr std::uint32_t k_no_entry = detail::keyed_refcount::k_no_entry;

    // Record one demand for a same-host ring; returns the post-increment count. The
    // 0->1 result is the gate the forwarder issues registry.acquire on (this wave
    // only books the demand; the acquire wires in a later wave).
    std::uint32_t acquire(std::string_view node_name, std::string_view fqn)
    {
        return m_refcount.bump(node_name, fqn);
    }

    // Drop one demand; returns the post-decrement count. The 1->0 result is the
    // gate the forwarder issues registry.release on. Returns the sentinel when the
    // pair is unknown so the caller treats it as "no transition".
    std::uint32_t release(std::string_view node_name, std::string_view fqn)
    {
        return m_refcount.drop(node_name, fqn);
    }

    // Whether this forwarder currently holds a same-host ring for the pair.
    [[nodiscard]] bool holds(std::string_view node_name, std::string_view fqn) const
    {
        return m_refcount.holds(node_name, fqn);
    }

private:
    detail::keyed_refcount m_refcount;
};

}

#endif
