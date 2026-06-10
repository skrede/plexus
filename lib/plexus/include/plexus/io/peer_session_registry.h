#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_REGISTRY_H

#include "plexus/io/node_name.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/reconnect.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/session_build_context.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include "plexus/detail/compat.h"

#include <map>
#include <chrono>
#include <memory>
#include <string>
#include <cstdint>
#include <optional>

namespace plexus::io {

// The multi-peer connection map: a node_id -> slot table where each slot bundles,
// in a deliberate member ORDER, the per-peer state a session is built from. The
// order IS the lifetime discipline: (1) the peer_context DATA record (the plan's
// driver-free value bundle), (2) the reconnect driver SIBLING (owned next to the
// record, NOT inside it — the record stays a pure value), then (3) the
// std::optional<peer_session> last. Because the optional session is declared last
// it is destroyed FIRST, so both the record (whose channel/epoch the session
// borrows) and the driver outlive every incarnation built from them — the
// cross-incarnation monotonicity and no-use-after-free are structural.
//
// On a dial success the engine hands the live channel to this registry, which
// stores it in the slot's record and builds the session from that record by one
// reference, then start()s it. Each slot's driver re-dials ONLY its own slot
// (its on_redial tears that slot's session down; the next on_dialed rebuilds it);
// the registry never iterates the map to reconnect the set — set-wide reconnect is
// a deferred concern, and the per-slot seam is kept minimal.
template <typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy> && transport_backend<Transport, Policy>
class peer_session_registry
{
public:
    using session_type = peer_session<Policy>;
    using driver_type = reconnect<Policy, Transport, Clock>;
    using channel_type = typename Policy::byte_channel_type;

    peer_session_registry(Transport &transport, session_build_context<Policy> &build)
        : m_transport(transport)
        , m_build(build)
    {
    }

    // Create the slot (record + its driver sibling) if absent. The driver is
    // constructed against the engine's transport, the slot's endpoint and the
    // node-wide redial_config; its on_redial tears down only this slot. Idempotent —
    // a second ensure_slot for a known id is a no-op.
    void ensure_slot(const node_id &id, const endpoint &ep, const std::string &node_name)
    {
        if(m_slots.find(id) != m_slots.end())
            return;
        if(endpoint_claimed(id, ep))
            return m_build.logger.warn("plexus: dial endpoint already claimed by another peer — slot not created");
        auto slot = std::make_unique<slot_block>(m_transport, m_build, id, ep, node_name);
        wire_redial(*slot, id);
        wire_dead(*slot, id);
        m_slots.emplace(id, std::move(slot));
    }

    // The shared dial-success tail (BOTH the lazy and eager knobs converge here): the
    // just-dialed channel is correlated to its slot BY THE ENDPOINT it dialed (the
    // transport hands that back) — NOT by arrival order, which a real async transport
    // reorders. Store it into the slot's record and build the session from the record
    // by one reference, then start() it. A completion for an endpoint with no live
    // slot (a torn-down peer) is dropped.
    void build_session_for_endpoint(const endpoint &ep, std::unique_ptr<channel_type> channel)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
                return build_into(*slot, std::move(channel), false);
    }

    // The inbound-bootstrap tail: a peer dialed US. We build an accepted session on
    // a slot keyed by a synthetic inbound identity (the peer's real node_id arrives
    // in the handshake; inbound slots are keyed by arrival order for now). Each
    // accepted peer gets a DISTINCT node_name derived from its synthetic id, so two
    // concurrently-accepted peers never share a forwarder refcount/demand key. The
    // slot is inserted into the map BEFORE the session is built and started, so any
    // wiring that looks the slot up by id finds it. No driver re-dials an accepted
    // slot — the dialer owns the redial.
    node_id accept_session(std::unique_ptr<channel_type> channel)
    {
        const node_id inbound_id = next_inbound_id();
        auto slot = std::make_unique<slot_block>(m_transport, m_build, inbound_id,
                                                 endpoint{}, node_name_of(inbound_id));
        auto [it, inserted] = m_slots.emplace(inbound_id, std::move(slot));
        if(!inserted)
        {
            m_build.logger.warn("plexus: inbound id collision — accept dropped");
            return inbound_id;
        }
        build_into(*it->second, std::move(channel), true);
        return inbound_id;
    }

    bool has_session(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->session.has_value();
    }

    bool is_connected(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->session.has_value()
            && it->second->session->is_complete();
    }

    session_type *session_for(const node_id &id)
    {
        auto it = m_slots.find(id);
        if(it == m_slots.end() || !it->second->session.has_value())
            return nullptr;
        return &*it->second->session;
    }

    // Iterate every CONNECTED peer (a slot with a complete session), calling
    // fn(id, session). It iterates the LIVE slot map only and skips an
    // incomplete/torn-down slot, so a tick-driven emit (the keepalive heartbeat) never
    // touches a dead session — the lifetime invariant. Mirrors is_connected's slot check.
    template <typename Fn>
    void for_each_connected(Fn &&fn)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->session && slot->session->is_complete())
                fn(id, *slot->session);
    }

    driver_type &driver_for(const node_id &id) { return m_slots.at(id)->driver; }
    std::uint32_t attempt_count(const node_id &id) const { return m_slots.at(id)->driver.attempt_count(); }

    // A peer is dead once its driver crossed a surrender bound (the per-slot flag
    // wire_dead latches — the slot is never erased there: that is a use-after-free).
    bool is_dead(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->dead;
    }

    // Route a per-endpoint dial failure to the slot that dialed it: many slots share
    // one transport, so the failure is correlated by endpoint, NOT by a per-driver
    // transport callback (which they would clobber). A failure for an unknown
    // endpoint (no live slot) is a no-op.
    void notify_dial_failed(const endpoint &ep)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
                return slot->driver.notify_dial_failed();
    }

private:
    // The per-peer slot: the member order encodes the outlive discipline (record,
    // then driver sibling, then the session built from both).
    struct slot_block
    {
        slot_block(Transport &transport, session_build_context<Policy> &build,
                   const node_id &id, const endpoint &ep, const std::string &node_name)
            : driver(transport, build.executor, build.redial, ep, build.redial_seed)
        {
            record.peer_id = id;
            record.node_name = node_name;
            record.dial_endpoint = ep;
        }

        peer_context<Policy> record;          // (1) the driver-free DATA record
        driver_type driver;                    // (2) the reconnect driver SIBLING
        std::optional<session_type> session;   // (3) built from the record, destroyed first
        bool dead{false};                      // latched when the driver surrenders
    };

    void build_into(slot_block &slot, std::unique_ptr<channel_type> channel, bool inbound)
    {
        slot.record.channel = std::move(channel);
        slot.session.reset();
        slot.session.emplace(slot.record, m_build.executor, m_build.fsm_cfg,
                             m_build.handshake_timeout, m_build.messages,
                             m_build.procedures, inbound, m_build.logger);
        if(inbound)
            wire_inbound_drop(slot, slot.record.peer_id);
        else
            wire_drop(slot, slot.record.peer_id);
        wire_lifecycle(slot);
        wire_stamp_seen(slot);
        slot.session->start();
    }

    // Route a dialed slot's transport drop to its OWN driver, BY id through the
    // registry (the session must not hold the driver directly). Set ONLY on dialed
    // slots — an accepted slot owns no redial, so routing its drop would advance a
    // driver that never dials.
    void wire_drop(slot_block &slot, const node_id &id)
    {
        slot.session->on_transport_drop([this, id] {
            auto it = m_slots.find(id);
            if(it != m_slots.end())
                it->second->driver.on_channel_dropped();
        });
    }

    // Route an accepted slot's transport drop to a plain tear_down — NO redial (the
    // dialer owns reconnection; an accepted peer that drops simply disconnects). The
    // tear_down fires disconnected through the session's was-complete guard, so an
    // accepted peer fires connected/disconnected/ready but never reconnect/dead.
    void wire_inbound_drop(slot_block &slot, const node_id &id)
    {
        slot.session->on_transport_drop([this, id] {
            auto it = m_slots.find(id);
            if(it != m_slots.end() && it->second->session)
                it->second->session->tear_down();
        });
    }

    // Route this slot's session lifecycle edges up to the engine's observer fan-out
    // via the node-shared build-context sink. Unlike wire_drop (dialed-only — an
    // accepted slot owns no redial), this is set for BOTH inbound and dialed slots:
    // an accepted peer still fires connected/disconnected/ready. The forward goes
    // straight to m_build.on_lifecycle (the engine sink) — no per-slot driver to
    // route to — so it needs no id indirection, only a re-check that the sink is set.
    void wire_lifecycle(slot_block &slot)
    {
        slot.session->on_lifecycle([this](const lifecycle_event &ev) {
            if(m_build.on_lifecycle)
                m_build.on_lifecycle(ev);
        });
    }

    // Route this slot's session presence stamps up to the engine's one liveliness
    // monitor via the node-shared build-context seam (set for both inbound and dialed
    // slots — either may receive a heartbeat). The session passes its own pinned peer
    // id; the forward re-checks the sink is set, mirroring wire_lifecycle.
    void wire_stamp_seen(slot_block &slot)
    {
        slot.session->on_stamp_seen([this](const node_id &id) {
            if(m_build.on_stamp_seen)
                m_build.on_stamp_seen(id);
        });
    }

    // Per-slot redial routing: the driver tears down THIS slot's dead session before
    // the fresh channel lands (the next on_dialed, correlated by endpoint, rebuilds
    // it). It tears down only this slot — the registry never loops the map to
    // reconnect the set.
    void wire_redial(slot_block &slot, const node_id &id)
    {
        slot.driver.on_redial([this, id] {
            auto it = m_slots.find(id);
            if(it != m_slots.end() && it->second->session)
                it->second->session->tear_down();
        });
    }

    // Latch a per-slot dead flag when the driver crosses a surrender bound. on_dead
    // fires from the driver's own report_dead stack frame while it lives in the slot,
    // so the slot is MARKED, never erased here — erasing it would free the driver
    // mid-callback (a use-after-free). is_dead exposes the flag.
    void wire_dead(slot_block &slot, const node_id &id)
    {
        slot.driver.on_dead([this, id] {
            auto it = m_slots.find(id);
            if(it == m_slots.end())
                return;
            it->second->dead = true;
            fire_dead(*it->second, id);
        });
    }

    // Fire the dead edge at the surrender latch (the session may already be torn
    // down, so it cannot own this edge — the slot does). The kind is dialed: drivers
    // are wired only on dialed slots (build_into gates wire_drop on !inbound and
    // accept_session constructs no driver), so on_dead structurally never fires for
    // an accepted slot — no inbound dead path exists. The slot is MARKED dead, never
    // erased here (erasing would free the driver mid-callback — a use-after-free).
    void fire_dead(slot_block &slot, const node_id &id)
    {
        if(!m_build.on_lifecycle)
            return;
        m_build.on_lifecycle(lifecycle_event{lifecycle_edge::dead, id,
                slot.record.node_name, peer_kind::dialed, handshake_outcome::none});
    }

    // Mint a fresh synthetic inbound identity. The counter is 64-bit and packed into
    // the id's low bytes, so the synthetic-id space is 2^64 wide — it does not wrap
    // after 256 accepts (a uint8 counter aliased a live slot's id, and the colliding
    // map insert then dropped the freshly-started session and its live channel).
    node_id next_inbound_id()
    {
        const std::uint64_t n = m_next_inbound++;
        node_id id{};
        for(int i = 0; i < 8; ++i)
            id[15 - i] = std::byte{static_cast<std::uint8_t>(n >> (8 * i))};
        return id;
    }

    // The dial→slot correlation key is the endpoint (the transport hands back the
    // endpoint a channel dialed), which is unambiguous only if at most one live slot
    // claims an endpoint: a second slot sharing it would steal the first's dial
    // completion. Reject the duplicate rather than mis-route silently.
    bool endpoint_claimed(const node_id &id, const endpoint &ep) const
    {
        for(const auto &[other, slot] : m_slots)
            if(other != id && slot->record.dial_endpoint == ep)
                return true;
        return false;
    }

    Transport &m_transport;
    session_build_context<Policy> &m_build;
    std::map<node_id, std::unique_ptr<slot_block>> m_slots;
    std::uint64_t m_next_inbound{1};
};

}

#endif
