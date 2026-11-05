#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_BUILD_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_BUILD_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/message_info.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/reliability_requirement.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/peer_session_slot.h"
#include "plexus/io/detail/peer_session_claim.h"
#include "plexus/io/detail/peer_session_dedup.h"
#include "plexus/io/detail/peer_session_redial.h"

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <string_view>

namespace plexus::io::detail {

// An accepted slot's drop tears down with no redial — the dialer owns reconnection.
template<typename Registry, typename Slot>
void wire_inbound_drop(Registry &reg, Slot &slot)
{
    slot.session->on_transport_drop(
            [&reg, &slot]
            {
                if(slot.session && !detail::close_if_superseded(reg, slot, peer_kind::accepted))
                    slot.session->tear_down();
            });
}

template<typename Registry, typename Slot>
void wire_observer(Registry &reg, Slot &slot)
{
    slot.session->on_lifecycle(
            [&reg, &slot](const lifecycle_event &ev)
            {
                detail::note_dial_connected(slot, ev);
                if(reg.m_build.on_lifecycle)
                    reg.m_build.on_lifecycle(ev);
                detail::track_identity_claim(reg, slot, ev);
            });
    slot.session->on_stamp_seen(
            [&reg](const node_id &id)
            {
                if(reg.m_build.on_stamp_seen)
                    reg.m_build.on_stamp_seen(id);
            });
    slot.session->set_declines_relay(reg.m_build.declines_relay);
    slot.session->on_decline_seen(
            [&reg](const node_id &id, bool declines)
            {
                if(reg.m_build.on_decline_seen)
                    reg.m_build.on_decline_seen(id, declines);
            });
    slot.session->on_security([&reg](const security_event &ev) { reg.m_build.session_observer.on_security(ev); });
}

template<typename Build, typename Slot>
void wire_frame_drops(Build &build, Slot &slot)
{
    slot.session->on_frame_dropped(
            [&build](const drop_event &ev)
            {
                if(build.on_frame_dropped)
                    build.on_frame_dropped(ev);
            });
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

// The session is destroyed before its channel is retired, and the seam and the build context outlive every
// incarnation, so the captured lower-channel reference stays valid. No factory leaves the hook absent, so a
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

template<typename Registry, typename Build, typename Slot>
void emplace_session(Registry &reg, Build &build, Slot &slot, bool inbound)
{
    handshake_fsm_config fsm_cfg = build.fsm_cfg;
    fsm_cfg.expected_peer_id     = detail::expected_identity(slot.record, inbound);
    slot.session.emplace(slot.record, build.executor, fsm_cfg, build.handshake_timeout, build.messages, build.procedures, inbound, build.logger);
    if(inbound)
        detail::wire_inbound_drop(reg, slot);
    else
        detail::wire_drop(reg, slot);
    detail::wire_observer(reg, slot);
    detail::wire_admit_complete(reg, build, slot);
    detail::wire_frame_drops(build, slot);
    detail::wire_message_route(reg, slot);
    detail::wire_object_route(reg, slot);
    detail::wire_security(reg, slot);
    if(scheme_is_best_effort(slot.record.channel->remote_endpoint().scheme))
        slot.session->set_subscribe_retry(build.subscribe_retry);
    slot.session->start();
}

}

#endif
