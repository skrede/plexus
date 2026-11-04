#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FILING_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FILING_H

#include "plexus/io/node_name.h"

#include "plexus/io/detail/peer_session_slot.h"
#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/node_id.h"

#include <utility>
#include <algorithm>

namespace plexus::io::detail {

// The map owns each slot through a unique_ptr and every closure holds its slot by reference, so moving a slot's entry
// to another key leaves those captures, and the reclaim's by-address erase, on the same slot.
struct slot_filing
{
    template<typename Registry>
    static node_id fresh_key(Registry &reg)
    {
        node_id id = inbound_slot_id(reg.m_next_inbound++);
        while(reg.m_slots.contains(id))
            id = inbound_slot_id(reg.m_next_inbound++);
        return id;
    }

    // A slot whose driver is closed and which holds no complete session serves nobody: a torn-down accepted slot
    // waiting for its erase.
    template<typename Slot>
    static bool out_of_service(Slot &slot)
    {
        return slot.driver.pending().closing() && !(slot.session && slot.session->is_complete());
    }

    template<typename Registry, typename Slot>
    static bool uncontested(Registry &reg, Slot &slot, const node_id &proven)
    {
        bool other = false;
        reg.for_each_connected([&](const node_id &, const typename Registry::session_type &s) { other = other || (&s != &*slot.session && s.peer_identity() == proven); });
        auto held = reg.m_slots.find(proven);
        return !other && (held == reg.m_slots.end() || out_of_service(*held->second));
    }

    // Frames a channel captured before the relabel keep the label they were captured under.
    template<typename Registry, typename Slot>
    static void relabel(Registry &reg, Slot &slot, const node_id &id)
    {
        rename(slot, id);
        if(reg.m_build.on_relabel && slot.record.channel)
            reg.m_build.on_relabel(*slot.record.channel, id);
    }

    template<typename Registry, typename Slot>
    static void vacate(Registry &reg, Slot &slot)
    {
        const node_id id = fresh_key(reg);
        move_to(reg, slot, id);
        rename(slot, id);
        slot.record.claim_released = true;
    }

    template<typename Registry, typename Slot>
    static void file_under_identity(Registry &reg, Slot &slot, const node_id &proven)
    {
        auto held = reg.m_slots.find(proven);
        if(held != reg.m_slots.end())
            vacate(reg, *held->second);
        move_to(reg, slot, proven);
    }

private:
    template<typename Slot>
    static void rename(Slot &slot, const node_id &id)
    {
        slot.record.peer_id   = id;
        slot.record.node_name = node_name_of(id);
        if(slot.session)
            detail::adopt_peer_name(*slot.session, slot.record.node_name);
    }

    template<typename Registry, typename Slot>
    static void move_to(Registry &reg, Slot &slot, const node_id &key)
    {
        auto it     = std::ranges::find_if(reg.m_slots, [&slot](const auto &entry) { return entry.second.get() == &slot; });
        auto entry  = reg.m_slots.extract(it);
        entry.key() = key;
        reg.m_slots.insert(std::move(entry));
    }
};

}

#endif
