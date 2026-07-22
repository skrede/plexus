#ifndef HPP_GUARD_PLEXUS_IO_FIXED_PEER_STORAGE_H
#define HPP_GUARD_PLEXUS_IO_FIXED_PEER_STORAGE_H

#include "plexus/node_id.h"

#include "plexus/detail/fail_closed.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/detail/route_admission.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io {

// The constrained-target (MCU) awareness backend: a dep-free, fixed-capacity flat array, a linear
// scan over N. Each identity holds a bounded, inline candidate list (never a heap container) with
// direct-first partitioning. The (N+1)-th DISTINCT identity calls plexus::detail::fail_closed — a
// DEFINED refusal, never an out-of-bounds write, a silent drop, or an undefined terminate.
template<std::size_t N, std::size_t Candidates = 4>
class fixed_peer_storage
{
public:
    bool put(const node_id &id, const endpoint &ep)
    {
        return put(id, ep, 0);
    }

    // A same-(id, endpoint) re-store refreshes the tick but reports no change: the idempotent
    // re-announce must not bump the graph observer's generation. A fresh direct endpoint overwrites
    // the single direct row in place.
    bool put(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        return admit(id, direct_candidate(ep), now, route_options{});
    }

    // Direct-first candidate admission: a direct row is never reject-and-counted, a transitive row is
    // admitted only into the non-reserved region and otherwise rejected-and-tallied — never displacing
    // a direct row under either protection scheme.
    bool admit(const node_id &id, const route_candidate &cand, std::uint64_t now, const route_options &opts)
    {
        entry *slot = find(id);
        if(slot == nullptr)
            slot = claim(id, cand.is_direct());
        if(slot == nullptr)
        {
            ++m_dropped;
            return false;
        }
        slot->last_refreshed = now;
        const auto r = detail::admit_candidate(slot->candidates.data(), slot->count, Candidates, cand, now, opts);
        if(r.dropped)
            ++m_dropped;
        return r.changed;
    }

    // Reported (via-only) admission: a new origin claims a free slot without fail_closed (a transitive
    // report never aborts the table), and the seq dedups against the row's embedded window.
    detail::report_admit note_reported(const node_id &id, const route_candidate &cand, std::uint16_t seq, std::uint64_t now, const route_options &opts)
    {
        entry *slot = find(id);
        if(slot == nullptr)
            slot = claim(id, false);
        if(slot == nullptr)
            return (++m_dropped, detail::report_admit::dropped);
        const auto r = detail::note_reported_row(slot->candidates.data(), slot->count, Candidates, slot->last_refreshed, cand, seq, now, opts);
        m_dropped += static_cast<std::size_t>(r == detail::report_admit::dropped);
        return r;
    }

    bool remove_transitive(const node_id &id, const node_id &via)
    {
        entry *slot = find(id);
        if(slot == nullptr || !detail::remove_transitive_row(slot->candidates.data(), slot->count, via))
            return false;
        if(slot->count == 0)
            slot->occupied = false;
        return true;
    }

    bool mark_unreachable_via(const node_id &id, const node_id &via)
    {
        entry *slot = find(id);
        return slot != nullptr && detail::mark_reachability_via(slot->candidates.data(), slot->count, via, graph::reachability::unreachable) != 0;
    }

    bool mark_reachable_via(const node_id &id, const node_id &via)
    {
        entry *slot = find(id);
        return slot != nullptr && detail::mark_reachability_via(slot->candidates.data(), slot->count, via, graph::reachability::reachable) != 0;
    }

    std::size_t reset_reported_windows(const node_id &via)
    {
        std::size_t n = 0;
        for(entry &e : m_slots)
            if(e.occupied)
                n += detail::reset_windows_via(e.candidates.data(), e.count, via);
        return n;
    }

    bool withdraw_seq_fresh(const node_id &id, const node_id &via, std::uint16_t seq)
    {
        entry *slot = find(id);
        return slot != nullptr && detail::admit_via_seq(slot->candidates.data(), slot->count, via, seq);
    }

    template<typename Fn>
    void for_each_origin_via(const node_id &via, Fn fn) const
    {
        for(const entry &e : m_slots)
            if(e.occupied)
                for(std::size_t i = 0; i < e.count; ++i)
                    if(!e.candidates[i].is_direct() && e.candidates[i].reach.via == via)
                    {
                        fn(e.id);
                        break;
                    }
    }

    void refresh(const node_id &id, std::uint64_t now)
    {
        if(entry *slot = find(id))
            slot->last_refreshed = now;
    }

    std::optional<endpoint> get(const node_id &id) const
    {
        if(const entry *slot = find(id))
            return detail::direct_endpoint(slot->candidates.data(), slot->count);
        return std::nullopt;
    }

    bool has(const node_id &id) const
    {
        return find(id) != nullptr;
    }

    std::span<const route_candidate> candidates(const node_id &id) const
    {
        if(const entry *slot = find(id))
            return {slot->candidates.data(), slot->count};
        return {};
    }

    bool remove(const node_id &id)
    {
        if(entry *slot = find(id))
        {
            slot->occupied = false;
            return true;
        }
        return false;
    }

    // Transitive candidates refused for want of a non-reserved row.
    std::size_t dropped() const noexcept
    {
        return m_dropped;
    }

    template<typename Report>
    void expire_older_than(std::uint64_t deadline, Report report)
    {
        for(entry &e : m_slots)
            if(e.occupied && e.last_refreshed < deadline)
            {
                report(e.id);
                e.occupied = false;
            }
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        for(const entry &e : m_slots)
            if(e.occupied)
                if(auto ep = detail::direct_endpoint(e.candidates.data(), e.count))
                    fn(e.id, *ep);
    }

    // Every candidate row of every occupied identity, direct and via-only alike, each with its own
    // reach and provenance — the extended-world enumeration a reported peer surfaces through. A
    // direct-only identity yields exactly its one direct row, so the direct-only output is byte-identical.
    template<typename Fn>
    void for_each_candidate(Fn fn) const
    {
        for(const entry &e : m_slots)
            for(std::size_t i = 0; e.occupied && i < e.count; ++i)
                fn(e.id, e.candidates[i].reach, e.candidates[i].origin);
    }

private:
    struct entry
    {
        node_id id{};
        std::array<route_candidate, Candidates> candidates{};
        std::size_t count{0};
        std::uint64_t last_refreshed{0};
        bool occupied{false};
    };

    // A new identity takes a free slot. When the identity array is full a DIRECT peer fails closed
    // (the target is sized below its direct peer count); a TRANSITIVE report is rejected by the caller
    // and counted, never allowed to exhaust the table or abort.
    entry *claim(const node_id &id, bool is_direct)
    {
        for(entry &e : m_slots)
            if(!e.occupied)
            {
                e.occupied = true;
                e.id       = id;
                e.count    = 0;
                return &e;
            }
        if(is_direct)
            plexus::detail::fail_closed("fixed_peer_storage: capacity exceeded");
        return nullptr;
    }

    entry *find(const node_id &id)
    {
        for(entry &e : m_slots)
            if(e.occupied && e.id == id)
                return &e;
        return nullptr;
    }
    const entry *find(const node_id &id) const
    {
        for(const entry &e : m_slots)
            if(e.occupied && e.id == id)
                return &e;
        return nullptr;
    }

    std::array<entry, N> m_slots{};
    std::size_t m_dropped{0};
};

}

#endif
