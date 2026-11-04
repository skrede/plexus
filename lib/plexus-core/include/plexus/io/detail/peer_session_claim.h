#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CLAIM_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_CLAIM_H

#include "plexus/io/endpoint.h"
#include "plexus/io/lifecycle_event.h"

#include "plexus/io/detail/peer_session_filing.h"

#include "plexus/node_id.h"

#include <ranges>
#include <algorithm>

namespace plexus::io::detail {

// A dialed slot whose peer proved an identity other than its card's leaves service and releases its endpoint to
// whichever identity answers there. Its card waits for a fresh one, which dials through a fresh slot.
template<typename Registry, typename Slot>
void track_identity_claim(Registry &reg, Slot &slot, const lifecycle_event &ev)
{
    if(ev.edge == lifecycle_edge::connected || ev.edge == lifecycle_edge::reconnected)
        slot.record.claim_released = false;
    if(!detail::is_misidentified_dial(ev))
        return;
    slot.record.claim_released = true;
    slot_filing::leave_service(reg, slot);
}

template<typename Registry>
bool endpoint_claimed(const Registry &reg, const node_id &id, const endpoint &ep)
{
    for(const auto &[other, slot] : reg.m_slots)
        if(other != id && slot->record.dial_endpoint == ep && !slot->record.claim_released)
            return true;
    return false;
}

// A dialed channel, or a failed dial, belongs to the slot whose driver is dialing its endpoint, else to the slot still
// claiming it, so a slot that released its endpoint only ever hears about the dial it made itself.
template<typename Slots>
std::ranges::iterator_t<Slots> attributed_slot(Slots &slots, const endpoint &ep)
{
    const auto at = [&ep](const auto &entry) { return entry.second->record.dial_endpoint == ep; };
    auto it       = std::ranges::find_if(slots, [&at](const auto &entry) { return at(entry) && entry.second->driver.dialing(); });
    if(it != std::ranges::end(slots))
        return it;
    return std::ranges::find_if(slots, [&at](const auto &entry) { return at(entry) && !entry.second->record.claim_released; });
}

}

#endif
