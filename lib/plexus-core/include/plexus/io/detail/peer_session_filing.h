#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FILING_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_FILING_H

#include "plexus/io/node_name.h"

#include "plexus/io/detail/peer_session_slot.h"
#include "plexus/io/detail/peer_session_reclaim.h"
#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/node_id.h"

#include <ranges>
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

    // A slot whose driver is closed and which holds no complete session serves nobody; it only waits for its erase.
    template<typename Slot>
    static bool out_of_service(Slot &slot)
    {
        return slot.driver.pending().closing() && !(slot.session && slot.session->is_complete());
    }

    template<typename Registry, typename Slot>
    static Slot *incumbent(Registry &reg, Slot &slot, const node_id &proven)
    {
        for(auto &[key, other] : reg.m_slots)
        {
            const auto &s = other->session;
            if(other.get() != &slot && s && s->is_complete() && s->peer_identity() == proven)
                return other.get();
        }
        return nullptr;
    }

    // Frames a channel captured before the relabel keep the label they were captured under.
    template<typename Registry, typename Slot>
    static void relabel(Registry &reg, Slot &slot, const node_id &id)
    {
        rename(slot, id);
        if(reg.m_build.on_relabel && slot.record.channel)
            reg.m_build.on_relabel(*slot.record.channel, id);
    }

    // The vacated key passes to a complete session already labeled with it that waits under another key.
    template<typename Registry, typename Slot>
    static void vacate(Registry &reg, Slot &slot)
    {
        const node_id key = entry_of(reg, slot)->first;
        const node_id id  = fresh_key(reg);
        move_to(reg, slot, id);
        rename(slot, id);
        slot.record.claim_released = true;
        hand_on(reg, key);
    }

    // A slot still in service keeps the key: the newcomer waits under its provisional key with the proven labels until
    // that slot leaves service.
    template<typename Registry, typename Slot>
    static void file_under_identity(Registry &reg, Slot &slot, const node_id &proven)
    {
        auto held = reg.m_slots.find(proven);
        if(held != reg.m_slots.end() && held->second.get() != &slot && out_of_service(*held->second))
            vacate(reg, *held->second);
        if(!reg.m_slots.contains(proven))
            move_to(reg, slot, proven);
    }

    template<typename Registry, typename Slot>
    static void take_identity(Registry &reg, Slot &slot, const node_id &proven)
    {
        relabel(reg, slot, proven);
        file_under_identity(reg, slot, proven);
    }

    template<typename Registry, typename Slot>
    static void leave_service(Registry &reg, Slot &slot)
    {
        slot_reclaim::arm(reg, slot);
        vacate(reg, slot);
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
    static std::ranges::iterator_t<decltype(Registry::m_slots)> entry_of(Registry &reg, Slot &slot)
    {
        return std::ranges::find_if(reg.m_slots, [&slot](const auto &entry) { return entry.second.get() == &slot; });
    }

    template<typename Registry, typename Slot>
    static void move_to(Registry &reg, Slot &slot, const node_id &key)
    {
        auto entry  = reg.m_slots.extract(entry_of(reg, slot));
        entry.key() = key;
        reg.m_slots.insert(std::move(entry));
    }

    template<typename Registry>
    static void hand_on(Registry &reg, const node_id &key)
    {
        const auto waits = [&key](const auto &entry)
        {
            const auto &s = *entry.second;
            return entry.first != key && s.record.peer_id == key && s.session && s.session->is_complete();
        };
        auto waiting = std::ranges::find_if(reg.m_slots, waits);
        if(waiting != reg.m_slots.end() && !reg.m_slots.contains(key))
            move_to(reg, *waiting->second, key);
    }
};

}

#endif
