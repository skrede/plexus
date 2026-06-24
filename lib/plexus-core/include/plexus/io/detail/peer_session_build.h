#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_BUILD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_BUILD_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/message_info.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/security_event.h"

#include <span>
#include <cstddef>
#include <string_view>

namespace plexus::io::detail {

// Route a dialed slot's transport drop to its OWN driver, BY id through the registry (the
// session must not hold the driver directly). Set ONLY on dialed slots — an accepted slot
// owns no redial, so routing its drop would advance a driver that never dials.
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

// Route an accepted slot's transport drop to a plain tear_down — NO redial (the dialer owns
// reconnection). The tear_down fires disconnected through the session's was-complete guard,
// so an accepted peer fires connected/disconnected/ready but never reconnect/dead.
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

// Route this slot's session events up to the engine through the node-shared build context:
// the lifecycle edge and the presence stamp ride their typed sinks, the security edge rides
// the one shared observer. Set for BOTH inbound and dialed slots. build_into re-runs on every
// reconnect rebuild, so a single installed observer survives reconnect.
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

// Thread the node-shared receive route into this slot's session, re-threaded on every
// reconnect rebuild — the per-session receive seam is lost on a rebuild, the shared route is
// not. The forward re-checks the sink is set.
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

// Thread the node-shared object-lane route, re-threaded on every reconnect rebuild.
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

// Borrow the node-level security seam (one per node) and thread the per-session AEAD
// decorator-install hook from the build-context factory (capturing THIS slot's just-built
// channel as the lower channel). The seam pointer, the build context and the slot's channel
// all outlive every incarnation, so the captured reference stays valid. When no factory is
// set the hook is left absent, so a security-engaged accept over plaintext is refused
// fail-closed rather than proceeding in cleartext.
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

// Per-slot redial routing: the driver tears down THIS slot's dead session before the fresh
// channel lands (the next on_dialed, correlated by endpoint, rebuilds it). It tears down only
// this slot — the registry never loops the map to reconnect the set.
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

// Fire the dead edge at the surrender latch (the session may already be torn down, so the
// slot owns this edge). The kind is dialed: drivers are wired only on dialed slots, so this
// structurally never fires for an accepted slot. The slot is MARKED dead, never erased here.
template<typename Registry, typename Slot>
void fire_dead(Registry &reg, Slot &slot, const node_id &id)
{
    if(!reg.m_build.on_lifecycle)
        return;
    reg.m_build.on_lifecycle(lifecycle_event{lifecycle_edge::dead, id, slot.record.node_name, peer_kind::dialed, handshake_outcome::none});
}

// Latch a per-slot dead flag when the driver crosses a surrender bound. on_dead fires from
// the driver's own report_dead stack frame while it lives in the slot, so the slot is MARKED,
// never erased here — erasing it would free the driver mid-callback (a use-after-free).
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
