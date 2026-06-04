#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_REGISTRY_H

#include "plexus/io/reconnect.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include "plexus/detail/compat.h"

#include <map>
#include <chrono>
#include <memory>
#include <string>
#include <optional>

namespace plexus::io {

// The node-shared inputs every per-slot session is built from: the engine owns one
// of these and the registry borrows it by reference. A slot rebuild (a reconnect)
// draws the same forwarders, self identity, handshake bound, executor and logger
// from here, so the node-wide wiring is reused with no re-plumbing.
template <typename Policy>
struct session_build_context
{
    using executor_type = typename Policy::executor_type;

    executor_type executor;
    handshake_fsm_config fsm_cfg;
    std::chrono::nanoseconds handshake_timeout;
    message_forwarder<Policy> &messages;
    procedure_forwarder<Policy> &procedures;
    reconnect_config redial;
    std::uint64_t redial_seed;
    log::logger &logger;
};

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
        auto slot = std::make_unique<slot_block>(m_transport, m_build, id, ep, node_name);
        wire_redial(*slot, id);
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
    // in the handshake; inbound slots are keyed by arrival order for now). No driver
    // re-dials an accepted slot — the dialer owns the redial.
    node_id accept_session(std::unique_ptr<channel_type> channel)
    {
        node_id inbound_id{};
        inbound_id[15] = std::byte{m_next_inbound++};
        auto slot = std::make_unique<slot_block>(m_transport, m_build, inbound_id,
                                                 endpoint{}, "inbound-peer");
        build_into(*slot, std::move(channel), true);
        m_slots.emplace(inbound_id, std::move(slot));
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

    driver_type &driver_for(const node_id &id) { return m_slots.at(id)->driver; }
    std::uint32_t attempt_count(const node_id &id) const { return m_slots.at(id)->driver.attempt_count(); }

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
    };

    void build_into(slot_block &slot, std::unique_ptr<channel_type> channel, bool inbound)
    {
        slot.record.channel = std::move(channel);
        slot.session.reset();
        slot.session.emplace(slot.record, m_build.executor, m_build.fsm_cfg,
                             m_build.handshake_timeout, m_build.messages,
                             m_build.procedures, inbound, m_build.logger);
        slot.session->start();
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

    Transport &m_transport;
    session_build_context<Policy> &m_build;
    std::map<node_id, std::unique_ptr<slot_block>> m_slots;
    std::uint8_t m_next_inbound{1};
};

}

#endif
