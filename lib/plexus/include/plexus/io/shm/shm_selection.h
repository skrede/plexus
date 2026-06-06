#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_SELECTION_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_SELECTION_H

#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/shm/same_host.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

namespace plexus::io::shm {

// The medium a same-host (peer, topic) pair resolves to. shm = the pair is
// same-host AND carries a qualifying hint, so each side attempts the ring acquire.
// stream = the pair stays on the local stream (AF_UNIX) — the fallback. The wire
// attach is NEVER suppressed regardless of this verdict (D-04 dual-delivery): a
// shared-memory upgrade ADDS a same-host fast path, it does not REPLACE the wire
// path, so a peer that cannot map the ring (or a third process that never upgrades)
// still receives over the stream. This enum only records which fast path, if any,
// the pair additionally attempts.
enum class same_host_medium : std::uint8_t
{
    stream,
    shm,
};

// The pure selection decision (D-01/D-02/D-03): a (peer, topic) pair is
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

// The per-forwarder acquired-ring bookkeeping: the set of (node_name, fqn) pairs
// this forwarder currently holds a same-host ring for, plus the per-pair refcount
// that gates the 0->1 acquire and the 1->0 release. This is borrowed BY REFERENCE
// by the selection caller (no singleton — each forwarder owns its own set, the
// vagus discipline), and it keys on the plexus (node_name, fqn) identity mirroring
// subscriber_registry. It holds only the bookkeeping; the registry.acquire call
// that maps the actual ring wires in later. The refcount is a structural copy of
// subscriber_registry's (peer, fqn) refcount shape so the two stay consistent.
class acquired_ring_set
{
public:
    // Record one demand for a same-host ring; returns the post-increment count. The
    // 0->1 result is the gate the forwarder issues registry.acquire on (this wave
    // only books the demand; the acquire wires in a later wave).
    std::uint32_t acquire(std::string_view node_name, std::string_view fqn)
    {
        auto &per_peer = m_refcount[std::string{node_name}];
        auto [it, inserted] = per_peer.try_emplace(std::string{fqn}, 0u);
        return ++it->second;
    }

    // Drop one demand; returns the post-decrement count. The 1->0 result is the
    // gate the forwarder issues registry.release on. Returns the sentinel when the
    // pair is unknown so the caller treats it as "no transition".
    std::uint32_t release(std::string_view node_name, std::string_view fqn)
    {
        auto peer_it = m_refcount.find(std::string{node_name});
        if(peer_it == m_refcount.end())
            return k_no_entry;
        auto fqn_it = peer_it->second.find(std::string{fqn});
        if(fqn_it == peer_it->second.end())
            return k_no_entry;
        std::uint32_t remaining = --fqn_it->second;
        if(remaining == 0)
        {
            peer_it->second.erase(fqn_it);
            if(peer_it->second.empty())
                m_refcount.erase(peer_it);
        }
        return remaining;
    }

    // Whether this forwarder currently holds a same-host ring for the pair.
    [[nodiscard]] bool holds(std::string_view node_name, std::string_view fqn) const
    {
        auto peer_it = m_refcount.find(std::string{node_name});
        if(peer_it == m_refcount.end())
            return false;
        auto fqn_it = peer_it->second.find(std::string{fqn});
        return fqn_it != peer_it->second.end() && fqn_it->second > 0;
    }

    static constexpr std::uint32_t k_no_entry = ~0u;

private:
    std::unordered_map<std::string, std::unordered_map<std::string, std::uint32_t>> m_refcount;
};

}

#endif
