#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FIXED_PEER_ADMISSION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FIXED_PEER_ADMISSION_H

#include "plexus/io/detail/route_admission.h"

#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"

#include "plexus/node_id.h"

#include <cstddef>
#include <cstdint>

namespace plexus::io::detail {

template<typename Slots>
typename Slots::value_type *claim_identity_slot(Slots &slots, const node_id &id)
{
    for(auto &e : slots)
        if(!e.occupied)
        {
            e.occupied        = true;
            e.id              = id;
            e.count           = 0;
            e.binding_invalid = false;
            return &e;
        }
    return nullptr;
}

// A direct candidate is never displaced from or rejected by an identity's candidate rows, while a
// transitive one takes only a non-reserved row. A new identity on a full table is rejected and
// counted whatever its kind, so an announced card can neither abort the node nor evict a peer.
template<typename S>
bool admit_identity_candidate(S &s, const node_id &id, const route_candidate &cand, std::uint64_t now, const route_options &opts)
{
    auto *slot = s.find(id);
    if(slot == nullptr)
        slot = detail::claim_identity_slot(s.m_slots, id);
    if(slot == nullptr)
    {
        ++s.m_dropped;
        return false;
    }
    slot->last_refreshed = now;
    const auto r         = detail::admit_candidate(slot->candidates.data(), slot->count, slot->candidates.size(), cand, now, opts);
    if(r.dropped)
        ++s.m_dropped;
    return r.changed;
}

// Reported (via-only) admission: a new origin claims a free slot or is counted, and the seq dedups
// against the row's embedded window.
template<typename S>
detail::report_admit note_identity_report(S &s, const node_id &id, const route_candidate &cand, std::uint16_t seq, std::uint64_t now, const route_options &opts)
{
    auto *slot = s.find(id);
    if(slot == nullptr)
        slot = detail::claim_identity_slot(s.m_slots, id);
    if(slot == nullptr)
        return (++s.m_dropped, detail::report_admit::dropped);
    const auto r = detail::note_reported_row(slot->candidates.data(), slot->count, slot->candidates.size(), slot->last_refreshed, cand, seq, now, opts);
    s.m_dropped += static_cast<std::size_t>(r == detail::report_admit::dropped);
    return r;
}

}

#endif
