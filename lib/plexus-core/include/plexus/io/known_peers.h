#ifndef HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H
#define HPP_GUARD_PLEXUS_IO_KNOWN_PEERS_H

#include "plexus/io/endpoint.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/std_map_peer_storage.h"

#include "plexus/io/detail/route_admission.h"

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

// The locked discovery-awareness TTL: an entry not re-announced (or refreshed by a heartbeat)
// within this window is aged out of awareness. The value is the smallest TTL that survives
// several missed announce periods at the conservative ~1-3s announce cadence — set by the
// recorded (announce_period, TTL) virtual-clock sweep, not guessed.
constexpr std::uint64_t default_discovery_ttl_ns = 15'000'000'000ull;

// The in-memory awareness table, keyed by node_id (not a discovery name): AWARENESS, not
// connectivity — note_peer records reachability and NEVER dials; the dial is a separate
// demand-driven act the routing_engine owns.
template<typename Storage = std_map_peer_storage>
class basic_known_peers
{
public:
    bool note_peer(const node_id &id, const endpoint &ep)
    {
        return m_storage.put(id, ep);
    }

    bool note_peer(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        return m_storage.put(id, ep, now);
    }

    // A reported (via-only) transitive candidate about a THIRD-party origin, deduplicated on the
    // per-origin seq embedded in its row; the direct-peer awareness is never perturbed.
    detail::report_admit note_reported(const node_id &id, const route_candidate &cand, std::uint16_t seq, std::uint64_t now, const route_options &opts)
    {
        return m_storage.note_reported(id, cand, seq, now, opts);
    }
    bool remove_transitive(const node_id &id, const node_id &via)
    {
        return m_storage.remove_transitive(id, via);
    }

    // Degrade the origin's row reaching it via `via` to unreachable in place (the relay carrying it
    // died): the identity and its via edge are retained, distinguishing UNREACHABLE-NOT-DEAD from a
    // retired row. True only when a row actually transitions.
    bool mark_unreachable_via(const node_id &id, const node_id &via)
    {
        return m_storage.mark_unreachable_via(id, via);
    }

    // The recovery counterpart: restore the origin's row via `via` to reachable when the path returns.
    bool mark_reachable_via(const node_id &id, const node_id &via)
    {
        return m_storage.mark_reachable_via(id, via);
    }

    // Re-arm the dedup windows of every row reaching an origin via `via` (its reporter session
    // (re)completed): the reporter's next report re-anchors on its current seq counter.
    std::size_t reset_reported_windows(const node_id &via)
    {
        return m_storage.reset_reported_windows(via);
    }

    // Seq-validate a withdrawal against the (id, via) row's window without removing it: fresh only
    // when the seq advances, so a stale/replayed withdrawal is rejected before it can retire a row.
    bool withdraw_seq_fresh(const node_id &id, const node_id &via, std::uint16_t seq)
    {
        return m_storage.withdraw_seq_fresh(id, via, seq);
    }

    template<typename Fn>
    void for_each_origin_via(const node_id &via, Fn fn) const
    {
        m_storage.for_each_origin_via(via, std::move(fn));
    }
    std::span<const route_candidate> candidates(const node_id &id) const
    {
        return m_storage.candidates(id);
    }

    // Extends an EXISTING entry's freshness; a no-op for an unknown id (a heartbeat from a
    // non-discovered peer must not invent awareness).
    void refresh(const node_id &id, std::uint64_t now)
    {
        m_storage.refresh(id, now);
    }

    std::optional<endpoint> lookup(const node_id &id) const
    {
        return m_storage.get(id);
    }

    bool contains(const node_id &id) const
    {
        return m_storage.has(id);
    }

    bool forget(const node_id &id)
    {
        return m_storage.remove(id);
    }

    template<typename Report>
    void expire_older_than(std::uint64_t deadline, Report report)
    {
        m_storage.expire_older_than(deadline, std::move(report));
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        m_storage.for_each(std::move(fn));
    }

    template<typename Fn>
    void for_each_candidate(Fn fn) const
    {
        m_storage.for_each_candidate(std::move(fn));
    }

private:
    Storage m_storage;
};

using known_peers = basic_known_peers<>;

}

#endif
