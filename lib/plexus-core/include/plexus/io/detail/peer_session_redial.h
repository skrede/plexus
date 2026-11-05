#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REDIAL_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_REDIAL_H

#include "plexus/io/peer_kind.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/disconnect_cause.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/peer_session_dedup.h"

namespace plexus::io::detail {

// Set only on dialed slots — an accepted slot owns no redial. The slot outlives the session that holds
// this closure, and keeps its address when it is re-filed under another key.
template<typename Registry, typename Slot>
void wire_drop(Registry &reg, Slot &slot)
{
    slot.session->on_transport_drop(
            [&reg, &slot]
            {
                if(detail::close_if_superseded(reg, slot, peer_kind::dialed))
                    return;
                slot.driver.on_channel_dropped();
                // A denied driver never redials, and only a redial tears a dropped session down.
                if(slot.driver.redial_denied() && slot.session && !slot.session->pending().closing())
                    slot.session->tear_down();
            });
}

template<typename Slot>
void note_dial_connected(Slot &slot, const lifecycle_event &ev)
{
    if(ev.kind != peer_kind::dialed || (ev.edge != lifecycle_edge::connected && ev.edge != lifecycle_edge::reconnected))
        return;
    slot.dead = false;
    slot.driver.note_connected();
}

template<typename Slot>
void wire_redial(Slot &slot)
{
    slot.driver.on_redial(
            [&slot]
            {
                if(slot.session)
                    slot.session->tear_down();
            });
}

// The session may already be torn down, so the slot owns this edge.
template<typename Registry, typename Slot>
void fire_dead(Registry &reg, Slot &slot)
{
    if(!reg.m_build.on_lifecycle)
        return;
    reg.m_build.on_lifecycle(
            lifecycle_event{lifecycle_edge::dead, slot.record.peer_id, slot.record.node_name, peer_kind::dialed, handshake_outcome::none, disconnect_cause::unspecified});
}

// on_dead fires from the driver's own report_dead stack frame while it lives in the slot, so
// the slot is marked dead, never erased here — erasing would free the driver mid-callback.
// A session the surrender leaves live is torn down first, so its disconnected edge precedes the dead edge.
template<typename Registry, typename Slot>
void wire_dead(Registry &reg, Slot &slot)
{
    slot.driver.on_dead(
            [&reg, &slot]
            {
                if(slot.session && !slot.session->pending().closing())
                    slot.session->tear_down();
                slot.dead = true;
                fire_dead(reg, slot);
            });
}

}

#endif
