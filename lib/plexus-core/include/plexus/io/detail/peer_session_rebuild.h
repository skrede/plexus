#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REBUILD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REBUILD_H

#include "plexus/io/endpoint.h"
#include "plexus/io/node_name.h"

#include "plexus/io/detail/peer_session_slot.h"
#include "plexus/io/detail/peer_session_build.h"

#include "plexus/node_id.h"

#include <memory>
#include <utility>
#include <type_traits>

namespace plexus::io::detail {

template<typename Registry, typename Build, typename Slot>
void rebuild_session(Registry &reg, Build &build, Slot &slot, std::unique_ptr<typename Slot::channel_type> channel, bool inbound);

template<typename Registry, typename Build, typename Slot>
void resume_parked(Registry &reg, Build &build, Slot &slot)
{
    // Only a dialed slot parks: an accepted slot is always built fresh.
    if(slot.parked)
        rebuild_session(reg, build, slot, std::move(slot.parked), false);
}

template<typename Registry, typename Build, typename Slot>
void post_resume(Registry &reg, Build &build, Slot &slot)
{
    Slot::policy_type::post(build.executor, [&reg, &build, &slot] { resume_parked(reg, build, slot); });
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
    // The dial stays in flight while a channel is parked, so a second completion is refused like an unclaimed one.
    if(slot.parked)
        return;
    if(slot.session && !slot.session->pending().closing())
        slot.session->tear_down();
    if(rebuild_blocked(slot))
        return park(reg, build, slot, std::move(channel));
    // The session goes before the channel it borrows.
    slot.session.reset();
    slot.retired.retire(std::move(slot.record.channel), [&reg, &build, &slot] { post_resume(reg, build, slot); });
    slot.record.channel = std::move(channel);
    slot.driver.mark_dial_settled();
    emplace_session(reg, build, slot, inbound);
}

template<typename Registry, typename Slot>
void build_session_into(Registry &reg, Slot &slot, std::unique_ptr<typename Registry::channel_type> channel, bool inbound)
{
    rebuild_session(reg, reg.m_build, slot, std::move(channel), inbound);
}

template<typename Registry>
bool endpoint_claimed(const Registry &reg, const node_id &id, const endpoint &ep)
{
    for(const auto &[other, slot] : reg.m_slots)
        if(other != id && slot->record.dial_endpoint == ep)
            return true;
    return false;
}

template<typename Registry>
void build_for_endpoint(Registry &reg, const endpoint &ep, std::unique_ptr<typename Registry::channel_type> channel)
{
    for(auto &[id, slot] : reg.m_slots)
        if(slot->record.dial_endpoint == ep)
        {
            if(slot->driver.redial_denied())
                return;
            return detail::build_session_into(reg, *slot, std::move(channel), false);
        }
}

template<typename Registry, typename PreBuild>
node_id accept_into_slot(Registry &reg, std::unique_ptr<typename Registry::channel_type> channel, PreBuild &&pre_build)
{
    using slot_block         = typename Registry::slot_block;
    const node_id inbound_id = detail::inbound_slot_id(reg.m_next_inbound++);
    auto slot                = std::make_unique<slot_block>(reg.m_transport, reg.m_build, inbound_id, endpoint{}, node_name_of(inbound_id));
    auto [it, inserted]      = reg.m_slots.emplace(inbound_id, std::move(slot));
    if(!inserted)
    {
        reg.m_build.logger.warn("plexus: inbound id collision — accept dropped");
        return inbound_id;
    }
    if constexpr(!std::is_null_pointer_v<std::decay_t<PreBuild>>)
        pre_build(*channel, inbound_id);
    detail::build_session_into(reg, *it->second, std::move(channel), true);
    return inbound_id;
}

}

#endif
