#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FIXED_PEER_ADMISSION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FIXED_PEER_ADMISSION_H

#include "plexus/io/detail/route_admission.h"

#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include "plexus/node_id.h"

#include "plexus/detail/fail_closed.h"

#include <cstddef>
#include <cstdint>

namespace plexus::io::detail {

// A new identity takes a free slot. When the identity array is full a DIRECT peer fails closed
// (the target is sized below its direct peer count); a TRANSITIVE report is rejected by the caller
// and counted, never allowed to exhaust the table or abort.
template<typename Slots>
typename Slots::value_type *claim_identity_slot(Slots &slots, const node_id &id, bool is_direct)
{
    for(auto &e : slots)
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

// Direct-first candidate admission: a direct row is never reject-and-counted, a transitive row is
// admitted only into the non-reserved region and otherwise rejected-and-tallied — never displacing
// a direct row under either protection scheme.
template<typename S>
bool admit_identity_candidate(S &s, const node_id &id, const route_candidate &cand, std::uint64_t now, const route_options &opts)
{
    auto *slot = s.find(id);
    if(slot == nullptr)
        slot = detail::claim_identity_slot(s.m_slots, id, cand.is_direct());
    if(slot == nullptr)
    {
        ++s.m_dropped;
        return false;
    }
    slot->last_refreshed = now;
    const auto r = detail::admit_candidate(slot->candidates.data(), slot->count, slot->candidates.size(), cand, now, opts);
    if(r.dropped)
        ++s.m_dropped;
    return r.changed;
}

// Reported (via-only) admission: a new origin claims a free slot without fail_closed (a transitive
// report never aborts the table), and the seq dedups against the row's embedded window.
template<typename S>
detail::report_admit note_identity_report(S &s, const node_id &id, const route_candidate &cand, std::uint16_t seq, std::uint64_t now, const route_options &opts)
{
    auto *slot = s.find(id);
    if(slot == nullptr)
        slot = detail::claim_identity_slot(s.m_slots, id, false);
    if(slot == nullptr)
        return (++s.m_dropped, detail::report_admit::dropped);
    const auto r = detail::note_reported_row(slot->candidates.data(), slot->count, slot->candidates.size(), slot->last_refreshed, cand, seq, now, opts);
    s.m_dropped += static_cast<std::size_t>(r == detail::report_admit::dropped);
    return r;
}

}

#endif
