#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_BUILD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_BUILD_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/message_info.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"

#include <span>
#include <cstddef>
#include <string_view>

namespace plexus::io::detail {

// Routed by id through the registry: the session must not hold the driver directly. Set only
// on dialed slots — an accepted slot owns no redial.
template<typename Registry, typename Slot>
void wire_drop(Registry &reg, Slot &slot, const node_id &id)
{
    slot.session->on_transport_drop(
            [&reg, id]
            {
                auto it = reg.m_slots.find(id);
                if(it != reg.m_slots.end())
                    it->second->driver.on_channel_dropped();
            });
}

// An accepted slot's drop tears down with no redial — the dialer owns reconnection.
template<typename Registry, typename Slot>
void wire_inbound_drop(Registry &reg, Slot &slot, const node_id &id)
{
    slot.session->on_transport_drop(
            [&reg, id]
            {
                auto it = reg.m_slots.find(id);
                if(it != reg.m_slots.end() && it->second->session)
                    it->second->session->tear_down();
            });
}

template<typename Registry, typename Slot>
void wire_observer(Registry &reg, Slot &slot)
{
    slot.session->on_lifecycle(
            [&reg](const lifecycle_event &ev)
            {
                if(reg.m_build.on_lifecycle)
                    reg.m_build.on_lifecycle(ev);
            });
    slot.session->on_stamp_seen(
            [&reg](const node_id &id)
            {
                if(reg.m_build.on_stamp_seen)
                    reg.m_build.on_stamp_seen(id);
            });
    slot.session->on_security([&reg](const security_event &ev) { reg.m_build.session_observer.on_security(ev); });
}

// Re-threaded on every reconnect rebuild: the per-session receive seam is lost on a rebuild,
// the node-shared route is not.
template<typename Registry, typename Slot>
void wire_message_route(Registry &reg, Slot &slot)
{
    slot.session->on_message_route(
            [&reg](std::string_view fqn, std::span<const std::byte> data, const message_info &mi)
            {
                if(reg.m_build.on_message)
                    reg.m_build.on_message(fqn, data, mi);
            });
}

template<typename Registry, typename Slot>
void wire_object_route(Registry &reg, Slot &slot)
{
    slot.session->on_object_route(
            [&reg](std::string_view fqn, const object_carrier &c)
            {
                if(reg.m_build.on_object)
                    reg.m_build.on_object(fqn, c);
            });
}

// The seam, the build context, and the slot's channel all outlive every incarnation, so the
// captured lower-channel reference stays valid. No factory leaves the hook absent, so a
// security-engaged accept over plaintext is refused fail-closed rather than running in cleartext.
template<typename Registry, typename Slot>
void wire_security(Registry &reg, Slot &slot)
{
    using channel_type = typename Registry::channel_type;
    slot.session->set_security_seam(&reg.m_build.install_security);
    if(reg.m_build.install_security_factory)
    {
        channel_type &lower = *slot.record.channel;
        slot.session->on_install_security([&reg, &lower](const security_negotiation &neg) { reg.m_build.install_security_factory(lower, neg); });
    }
}

template<typename Registry, typename Slot>
void wire_redial(Registry &reg, Slot &slot, const node_id &id)
{
    slot.driver.on_redial(
            [&reg, id]
            {
                auto it = reg.m_slots.find(id);
                if(it != reg.m_slots.end() && it->second->session)
                    it->second->session->tear_down();
            });
}

// The session may already be torn down, so the slot owns this edge.
template<typename Registry, typename Slot>
void fire_dead(Registry &reg, Slot &slot, const node_id &id)
{
    if(!reg.m_build.on_lifecycle)
        return;
    reg.m_build.on_lifecycle(lifecycle_event{lifecycle_edge::dead, id, slot.record.node_name, peer_kind::dialed, handshake_outcome::none});
}

// on_dead fires from the driver's own report_dead stack frame while it lives in the slot, so
// the slot is marked dead, never erased here — erasing would free the driver mid-callback.
template<typename Registry, typename Slot>
void wire_dead(Registry &reg, Slot &slot, const node_id &id)
{
    slot.driver.on_dead(
            [&reg, id]
            {
                auto it = reg.m_slots.find(id);
                if(it == reg.m_slots.end())
                    return;
                it->second->dead = true;
                fire_dead(reg, *it->second, id);
            });
}

}

#endif
