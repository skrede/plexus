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
#include "plexus/io/detail/peer_session_build.h"

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
#include <type_traits>

namespace plexus::io {

// over-limit: one cohesive multi-peer connection map; the public query/lifecycle verbs all
// reach the shared m_slots ownership table + the borrowed m_build context, so splitting the
// surface scatters that shared state (the session-build and seam-wiring helpers are extracted
// to detail/peer_session_build.h).
//
// The multi-peer connection map: a node_id -> slot table where each slot bundles, in a
// deliberate member ORDER, the per-peer state a session is built from. The order IS the
// lifetime discipline (record, then driver sibling, then the optional session last): the
// session is destroyed FIRST, so the record (whose channel/epoch it borrows) and the driver
// outlive every incarnation — cross-incarnation monotonicity and no-use-after-free are
// structural. Each slot's driver re-dials ONLY its own slot; the registry never iterates the
// map to reconnect the set (set-wide reconnect is a deferred concern). The session-build and
// seam-wiring helpers are extracted to detail/peer_session_build.h (relocation by friend).
template<typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy> && transport_backend<Transport, Policy>
class peer_session_registry
{
public:
    using session_type = peer_session<Policy>;
    using driver_type  = reconnect<Policy, Transport, Clock>;
    using channel_type = typename Policy::byte_channel_type;

    peer_session_registry(Transport &transport, session_build_context<Policy> &build)
            : m_transport(transport)
            , m_build(build)
    {
    }

    // Create the slot (record + its driver sibling) if absent. Idempotent — a second
    // ensure_slot for a known id is a no-op.
    void ensure_slot(const node_id &id, const endpoint &ep, const std::string &node_name)
    {
        if(m_slots.find(id) != m_slots.end())
            return;
        if(endpoint_claimed(id, ep))
            return m_build.logger.warn("plexus: dial endpoint already claimed by another peer — slot not created");
        auto slot = std::make_unique<slot_block>(m_transport, m_build, id, ep, node_name);
        detail::wire_redial(*this, *slot, id);
        detail::wire_dead(*this, *slot, id);
        m_slots.emplace(id, std::move(slot));
    }

    // The shared dial-success tail: the just-dialed channel is correlated to its slot BY THE
    // ENDPOINT it dialed (the transport hands that back) — NOT by arrival order, which a real
    // async transport reorders. A completion for an endpoint with no live slot is dropped.
    void build_session_for_endpoint(const endpoint &ep, std::unique_ptr<channel_type> channel)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
            {
                // Re-open the driver's in-flight gate so a subsequent drop can re-dial (a
                // redundant reach() during the dial window was a no-op — the double-dial guard).
                slot->driver.mark_dial_settled();
                return build_into(*slot, std::move(channel), false);
            }
    }

    // The inbound-bootstrap tail: a peer dialed US. Build an accepted session on a slot keyed
    // by a synthetic inbound identity. Each accepted peer gets a DISTINCT node_name so two
    // concurrently-accepted peers never share a forwarder refcount/demand key. The slot is
    // inserted BEFORE the session is built so any by-id wiring lookup finds it. No driver
    // re-dials an accepted slot — the dialer owns the redial.
    template<typename PreBuild = std::nullptr_t>
    node_id accept_session(std::unique_ptr<channel_type> channel, PreBuild &&pre_build = nullptr)
    {
        const node_id inbound_id = next_inbound_id();
        auto          slot       = std::make_unique<slot_block>(m_transport, m_build, inbound_id, endpoint{}, node_name_of(inbound_id));
        auto [it, inserted]      = m_slots.emplace(inbound_id, std::move(slot));
        if(!inserted)
        {
            m_build.logger.warn("plexus: inbound id collision — accept dropped");
            return inbound_id;
        }
        if constexpr(!std::is_null_pointer_v<std::decay_t<PreBuild>>)
            pre_build(*channel, inbound_id);
        build_into(*it->second, std::move(channel), true);
        return inbound_id;
    }

    // The peer node_id of the slot that dialed this endpoint, or absence if none claims it.
    // A reverse scan over the small slot map (the cold mint path, not a hot per-frame lookup).
    std::optional<node_id> id_for_endpoint(const endpoint &ep) const
    {
        for(const auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
                return id;
        return std::nullopt;
    }

    bool has_session(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->session.has_value();
    }

    bool is_connected(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->session.has_value() && it->second->session->is_complete();
    }

    session_type *session_for(const node_id &id)
    {
        auto it = m_slots.find(id);
        if(it == m_slots.end() || !it->second->session.has_value())
            return nullptr;
        return &*it->second->session;
    }

    // The session for a peer keyed by the forwarder node_name (the coordinator keys its
    // companion lanes by node_name). nullptr for an unknown name or an unbuilt session — a
    // drained frame for a vanished peer is then dropped.
    session_type *session_for_name(std::string_view node_name)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.node_name == node_name)
                return slot->session.has_value() ? &*slot->session : nullptr;
        return nullptr;
    }

    // The same-host verdict for a peer keyed by the forwarder node_name. Fail-closed: an
    // unknown name or an unbuilt session is NOT same-host (no ring acquire).
    [[nodiscard]] bool same_host_for(std::string_view node_name) const
    {
        for(const auto &[id, slot] : m_slots)
            if(slot->record.node_name == node_name)
                return slot->session.has_value() && slot->session->same_host();
        return false;
    }

    // Iterate every CONNECTED peer (a slot with a complete session), skipping an
    // incomplete/torn-down slot, so a tick-driven emit never touches a dead session.
    template<typename Fn>
    void for_each_connected(Fn &&fn)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->session && slot->session->is_complete())
                fn(id, *slot->session);
    }

    driver_type &driver_for(const node_id &id)
    {
        return m_slots.at(id)->driver;
    }
    std::uint32_t attempt_count(const node_id &id) const
    {
        return m_slots.at(id)->driver.attempt_count();
    }

    // A peer is dead once its driver crossed a surrender bound (the per-slot flag wire_dead
    // latches — the slot is never erased there: that would be a use-after-free).
    bool is_dead(const node_id &id) const
    {
        auto it = m_slots.find(id);
        return it != m_slots.end() && it->second->dead;
    }

    // Route a per-endpoint dial failure to the slot that dialed it: many slots share one
    // transport, so the failure is correlated by endpoint, NOT by a per-driver transport
    // callback (which they would clobber). A failure for an unknown endpoint is a no-op.
    void notify_dial_failed(const endpoint &ep)
    {
        for(auto &[id, slot] : m_slots)
            if(slot->record.dial_endpoint == ep)
                return slot->driver.notify_dial_failed();
    }

private:
    // The per-peer slot: the member order encodes the outlive discipline (record, then driver
    // sibling, then the session built from both).
    struct slot_block
    {
        slot_block(Transport &transport, session_build_context<Policy> &build, const node_id &id, const endpoint &ep, const std::string &node_name)
                : driver(transport, build.executor, build.redial, ep, build.redial_seed)
        {
            record.peer_id       = id;
            record.node_name     = node_name;
            record.dial_endpoint = ep;
        }

        peer_context<Policy>        record;      // (1) the driver-free DATA record
        driver_type                 driver;      // (2) the reconnect driver SIBLING
        std::optional<session_type> session;     // (3) built from the record, destroyed first
        bool                        dead{false}; // latched when the driver surrenders
    };

    void build_into(slot_block &slot, std::unique_ptr<channel_type> channel, bool inbound)
    {
        slot.record.channel = std::move(channel);
        slot.session.reset();
        slot.session.emplace(slot.record, m_build.executor, m_build.fsm_cfg, m_build.handshake_timeout, m_build.messages, m_build.procedures, inbound, m_build.logger);
        if(inbound)
            detail::wire_inbound_drop(*this, slot, slot.record.peer_id);
        else
            detail::wire_drop(*this, slot, slot.record.peer_id);
        detail::wire_observer(*this, slot);
        detail::wire_message_route(*this, slot);
        detail::wire_object_route(*this, slot);
        detail::wire_security(*this, slot);
        slot.session->start();
    }

    template<typename R, typename S>
    friend void detail::wire_drop(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::wire_inbound_drop(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::wire_observer(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_message_route(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_object_route(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_security(R &, S &);
    template<typename R, typename S>
    friend void detail::wire_redial(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::wire_dead(R &, S &, const node_id &);
    template<typename R, typename S>
    friend void detail::fire_dead(R &, S &, const node_id &);

    // Mint a fresh synthetic inbound identity. The counter is 64-bit and packed into the id's
    // low bytes, so the synthetic-id space is 2^64 wide — it does not wrap after 256 accepts
    // (a uint8 counter aliased a live slot's id, dropping a freshly-started session).
    node_id next_inbound_id()
    {
        const std::uint64_t n = m_next_inbound++;
        node_id             id{};
        for(int i = 0; i < 8; ++i)
            id[15 - i] = std::byte{static_cast<std::uint8_t>(n >> (8 * i))};
        return id;
    }

    // The dial→slot correlation key is the endpoint, unambiguous only if at most one live slot
    // claims it: a second slot sharing it would steal the first's dial completion. Reject the
    // duplicate rather than mis-route silently.
    bool endpoint_claimed(const node_id &id, const endpoint &ep) const
    {
        for(const auto &[other, slot] : m_slots)
            if(other != id && slot->record.dial_endpoint == ep)
                return true;
        return false;
    }

    Transport                                     &m_transport;
    session_build_context<Policy>                 &m_build;
    std::map<node_id, std::unique_ptr<slot_block>> m_slots;
    std::uint64_t                                  m_next_inbound{1};
};

}

#endif
