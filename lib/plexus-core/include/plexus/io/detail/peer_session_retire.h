#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_RETIRE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_RETIRE_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/disconnect_cause.h"

#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/peer_session_dedup.h"
#include "plexus/io/detail/peer_session_rebuild.h"
#include "plexus/io/detail/peer_session_lifecycle.h"

#include "plexus/node_id.h"

#include <memory>
#include <utility>

namespace plexus::io::detail {

template<typename Registry, typename Build, typename Slot>
void retire_into_cell(Registry &reg, Build &build, Slot &slot, std::unique_ptr<typename Slot::channel_type> channel)
{
    slot.retired.retire(std::move(channel), [&reg, &build, &slot] { post_resume(reg, build, slot); });
}

// A torn-down session is destroyed once its waits have completed; a live one is left to end on its own teardown.
template<typename Registry, typename Build, typename Slot>
void release_session(Registry &reg, Build &build, Slot &slot)
{
    if(!slot.session || !slot.session->pending().closing())
        return;
    if(slot.session->pending().settled())
        return slot.session.reset();
    slot.session->pending().on_settled([&reg, &build, &slot] { post_resume(reg, build, slot); });
}

template<typename Slot>
bool holds_nothing(Slot &slot)
{
    return slot.retired.empty() && !slot.parked && !slot.session && !slot.record.channel && slot.driver.pending().settled();
}

// A denied or closing slot builds nothing: its session goes first, then its channel and a parked one leave through the
// cell one at a time, each erase posting the next step.
template<typename Registry, typename Build, typename Slot>
void retire_step(Registry &reg, Build &build, Slot &slot)
{
    release_session(reg, build, slot);
    if(!slot.retired.empty())
        return;
    if(slot.parked)
        return retire_into_cell(reg, build, slot, std::move(slot.parked));
    if(slot.session)
        return;
    if(slot.record.channel)
        return retire_into_cell(reg, build, slot, std::move(slot.record.channel));
    if(!slot.driver.pending().settled())
        return;
    if(auto emptied = std::exchange(slot.on_emptied_cb, nullptr))
        emptied();
}

template<typename Registry, typename Build, typename Slot>
void withdraw_slot(Registry &reg, Build &build, Slot &slot)
{
    if(slot.session && !slot.session->pending().closing())
        slot.session->tear_down();
    retire_step(reg, build, slot);
}

template<typename Registry>
void hang_up_slot(Registry &reg, const node_id &id)
{
    auto it = reg.m_slots.find(id);
    if(it == reg.m_slots.end())
        return;
    it->second->driver.deny_redial();
    withdraw_slot(reg, reg.m_build, *it->second);
}

// A duplicate of a surviving session closes quietly. Otherwise the stale session is torn down at once, so its
// disconnected edge precedes the redial its dialed slot then schedules.
template<typename Registry>
void tear_down_stale_slot(Registry &reg, const node_id &key, disconnect_cause cause)
{
    auto it = reg.m_slots.find(key);
    if(it == reg.m_slots.end() || !it->second->session || !it->second->session->is_complete())
        return;
    auto &slot           = *it->second;
    const peer_kind kind = detail::session_kind(*slot.session);
    if(detail::close_if_superseded(reg, slot, kind))
        return;
    detail::tear_down_with_cause(*slot.session, cause);
    if(kind == peer_kind::dialed)
        slot.driver.on_channel_dropped();
}

// Every slot's redial is denied before any session is torn down, so no teardown can arm a redial. Each slot is then
// one operation of the owner until it holds nothing and its driver has settled; a slot already reclaimed waits as one of
// the registry's posted erases instead.
template<typename Registry>
void close_slots(Registry &reg, pending_ops &owner)
{
    for(auto &[id, slot] : reg.m_slots)
        slot->driver.close();
    for(auto &[id, slot] : reg.m_slots)
    {
        if(slot->posts.closing() && holds_nothing(*slot))
            continue;
        owner.begin();
        slot->on_emptied_cb = [&owner] { owner.end(); };
        slot->driver.pending().on_settled([&reg, &build = reg.m_build, &s = *slot] { post_resume(reg, build, s); });
        withdraw_slot(reg, reg.m_build, *slot);
    }
    hold_until_settled(owner, reg.m_reclaims);
    reg.m_reclaims.close();
}

}

#endif
