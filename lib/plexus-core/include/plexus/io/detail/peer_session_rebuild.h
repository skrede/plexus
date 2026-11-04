#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REBUILD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REBUILD_H

#include "plexus/io/endpoint.h"
#include "plexus/io/node_name.h"

#include "plexus/io/detail/peer_session_slot.h"
#include "plexus/io/detail/peer_session_build.h"
#include "plexus/io/detail/peer_session_claim.h"
#include "plexus/io/detail/peer_session_filing.h"
#include "plexus/io/detail/peer_session_reclaim.h"

#include "plexus/node_id.h"

#include <memory>
#include <utility>
#include <type_traits>

namespace plexus::io::detail {

template<typename Registry, typename Build, typename Slot>
void rebuild_session(Registry &reg, Build &build, Slot &slot, std::unique_ptr<typename Slot::channel_type> channel, bool inbound);

template<typename Registry, typename Build, typename Slot>
void retire_step(Registry &reg, Build &build, Slot &slot);

template<typename Registry, typename Build, typename Slot>
void retire_into_cell(Registry &reg, Build &build, Slot &slot, std::unique_ptr<typename Slot::channel_type> channel);

template<typename Registry>
void retire_unclaimed(Registry &reg, std::unique_ptr<typename Registry::channel_type> channel);

template<typename Registry, typename Build, typename Slot>
void resume_parked(Registry &reg, Build &build, Slot &slot)
{
    if(slot.driver.redial_denied())
        return retire_step(reg, build, slot);
    if(slot.parked)
        rebuild_session(reg, build, slot, std::move(slot.parked), false);
}

template<typename Registry, typename Build, typename Slot>
void post_resume(Registry &reg, Build &build, Slot &slot)
{
    slot.posts.begin();
    Slot::policy_type::post(build.executor,
                            [&reg, &build, &slot]
                            {
                                slot.posts.end();
                                resume_parked(reg, build, slot);
                            });
}

// A slot rebuilds in place only once its old session's waits have completed and its last retired channel has been
// erased, so whatever either of them still has queued lands on its own object, never on a successor.
template<typename Slot>
bool rebuild_blocked(Slot &slot)
{
    return (slot.session && !slot.session->pending().settled()) || !slot.retired.empty();
}

template<typename Registry, typename Build, typename Slot>
void park(Registry &reg, Build &build, Slot &slot, std::unique_ptr<typename Slot::channel_type> channel)
{
    slot.parked = std::move(channel);
    if(slot.session && !slot.session->pending().settled())
        slot.session->pending().on_settled([&reg, &build, &slot] { post_resume(reg, build, slot); });
}

template<typename Registry, typename Build, typename Slot>
void rebuild_session(Registry &reg, Build &build, Slot &slot, std::unique_ptr<typename Slot::channel_type> channel, bool inbound)
{
    // The dial stays in flight while a channel is parked, so a second completion leaves like an unclaimed one.
    if(slot.parked)
        return retire_unclaimed(reg, std::move(channel));
    if(slot.driver.redial_denied())
    {
        slot.parked = std::move(channel);
        return retire_step(reg, build, slot);
    }
    if(slot.session && !slot.session->pending().closing())
        slot.session->tear_down();
    if(rebuild_blocked(slot))
        return park(reg, build, slot, std::move(channel));
    // The session goes before the channel it borrows.
    slot.session.reset();
    retire_into_cell(reg, build, slot, std::move(slot.record.channel));
    slot.record.channel = std::move(channel);
    slot.driver.mark_dial_settled();
    emplace_session(reg, build, slot, inbound);
    slot.session->pending().on_settled([&reg, &build, &slot] { post_resume(reg, build, slot); });
}

template<typename Registry, typename Slot>
void build_session_into(Registry &reg, Slot &slot, std::unique_ptr<typename Registry::channel_type> channel, bool inbound)
{
    rebuild_session(reg, reg.m_build, slot, std::move(channel), inbound);
}

// The inbound id taken is one no slot holds, so an accepted slot is always fresh.
template<typename Registry>
typename Registry::slot_block &emplace_inbound_slot(Registry &reg)
{
    const node_id id = detail::slot_filing::fresh_key(reg);
    auto slot        = std::make_unique<typename Registry::slot_block>(reg.m_transport, reg.m_build, id, endpoint{}, node_name_of(id));
    return *reg.m_slots.emplace(id, std::move(slot)).first->second;
}

// A channel no slot claims leaves through a fresh slot of its own, keyed like an accept, whose cell retires it.
template<typename Registry>
void retire_unclaimed(Registry &reg, std::unique_ptr<typename Registry::channel_type> channel)
{
    auto &slot = emplace_inbound_slot(reg);
    detail::slot_reclaim::arm(reg, slot);
    rebuild_session(reg, reg.m_build, slot, std::move(channel), false);
}

template<typename Registry>
void build_for_endpoint(Registry &reg, const endpoint &ep, std::unique_ptr<typename Registry::channel_type> channel)
{
    auto it = detail::attributed_slot(reg.m_slots, ep);
    if(it == reg.m_slots.end())
        return retire_unclaimed(reg, std::move(channel));
    detail::build_session_into(reg, *it->second, std::move(channel), false);
}

template<typename Registry, typename PreBuild>
node_id accept_into_slot(Registry &reg, std::unique_ptr<typename Registry::channel_type> channel, PreBuild &&pre_build)
{
    auto &slot = emplace_inbound_slot(reg);
    if constexpr(!std::is_null_pointer_v<std::decay_t<PreBuild>>)
        pre_build(*channel, slot.record.peer_id);
    detail::build_session_into(reg, slot, std::move(channel), true);
    detail::slot_reclaim::arm(reg, slot);
    return slot.record.peer_id;
}

}

#endif
