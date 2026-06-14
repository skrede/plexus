#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_MUX_MEMBER_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_MUX_MEMBER_H

#include "plexus/io/shm/region_broker_concept.h"
#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/notifier_concept.h"
#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/shm_channel.h"
#include "plexus/io/shm/shm_slot_owner.h"
#include "plexus/io/shm/shm_topic_registry.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"
#include "plexus/wire/stream_inbound.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io::shm {

// The per-topic byte_channel the shm mux member hands up. It wraps ONE live ring
// (the registry's shm_channel for a (fqn, direction)) behind the seven byte_channel
// verbs the multiplexer's polymorphic_byte_channel erases: send memcpy-publishes
// into the slab and wakes the cross-process consumer; the registry's notifier-driven
// drain pumps received messages into on_data as header-on bytes (the on_data span
// contract). close() releases the ring back to the registry (the demand-driven
// refcount: at 1 -> 0 the registry tears the ring down + unlinks). on_closed /
// on_error are stored; on_protocol_close never fires (no byte-stream framing exists
// on a shared-memory ring, so no partial-frame violation is expressible). It borrows
// the registry + channel BY REFERENCE; the registry outlives every channel it mints.
template <typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_byte_channel
{
public:
    using registry_type = shm_topic_registry<Broker, Notifier>;

    shm_byte_channel(registry_type &registry, shm_channel<Notifier> &channel,
                     std::string fqn, endpoint remote) noexcept
        : m_registry(registry), m_channel(channel),
          m_fqn(std::move(fqn)), m_remote(std::move(remote))
    {
    }

    shm_byte_channel(const shm_byte_channel &) = delete;
    shm_byte_channel &operator=(const shm_byte_channel &) = delete;

    ~shm_byte_channel()
    {
        if(!m_released)
            m_registry.release(m_fqn, ring_direction::request);
    }

    // memcpy into the slab -> publish -> signal. An oversize payload surfaces
    // message_too_large; a stalled reliable path surfaces would_block. The channel
    // stays open either way (the bytes never left the node) -- the publisher learns
    // the message will not send rather than seeing a silent drop.
    void send(std::span<const std::byte> data)
    {
        const loan_status st = m_channel.send(data);
        if(st == loan_status::rejected)
        {
            if(m_on_error)
                m_on_error(io_error::message_too_large);
            emit_drop();
        }
        else if(st == loan_status::congested)
        {
            if(m_on_error)
                m_on_error(io_error::would_block);
            emit_drop();
        }
    }

    // Release the ring back to the registry (idempotent: a second close is a no-op).
    void close()
    {
        if(m_released)
            return;
        m_released = true;
        m_registry.release(m_fqn, ring_direction::request);
        if(m_on_closed)
            m_on_closed();
    }

    [[nodiscard]] endpoint remote_endpoint() const { return m_remote; }

    // Install the data sink and pump whatever is already pending: the registry arms
    // the ring's notifier with a drain-on-wake, but this channel routes the drained
    // bytes to ITS sink, so it re-drains here and on each pump() the owner drives off
    // a wake.
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
        pump();
    }

    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb) { m_on_error = std::move(cb); }
    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> cb) { m_on_drop = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    // send() memcpys straight into the shared-memory slab (no bounded userspace egress
    // queue): the bounded ring's own congestion verdict is surfaced inline at send time,
    // so this channel keeps no queued backlog. It therefore reports 0 — "always accepts" —
    // the accurate saturation signal for an in-slab fire-through channel, and the erasure
    // forwards it so a mux-composed shm member exposes the same occupancy read as a stream
    // member.
    [[nodiscard]] std::size_t backpressured() const noexcept { return 0; }

    // The stable per-construction egress key (the erasure forwards it so a mux-composed shm
    // member keys the scheduler band map exactly as a stream member). An in-slab fire-through
    // channel always admits (backpressured()==0), so its band is never saturated, but the key
    // is still distinct so a reconnect cannot alias a freed member's entry.
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept { return m_scheduler_key; }

    // Drain every pending message into on_data as header-on bytes. The owner drives
    // this on each notifier wake (the registry's drain is a discard; this delivers).
    void pump()
    {
        if(!m_on_data)
            return;
        typename shm_channel<Notifier>::deliver_fn deliver =
            [this](::plexus::wire_bytes<shm_slot_owner> wb) {
                m_on_data(std::span<const std::byte>{wb.data(), wb.size()});
            };
        m_channel.drain(deliver);
    }

private:
    // The shm ring's congestion/rejection verdict surfaces inline at send time (the ring
    // has no userspace egress queue to post from); the drop edge is therefore reported
    // straight off send(). It carries drop_cause::blocked (a congestion-drop) at the local
    // tier. The engine binds m_on_drop to its posted drop_sink at the dial/accept point, so
    // the synchronous emit here reaches the observer fan-out POSTED, never inline.
    void emit_drop()
    {
        if(m_on_drop)
            m_on_drop(detail::drop_event{.cause = detail::drop_cause::blocked,
                                         .transport = locality::local});
    }

    registry_type        &m_registry;
    shm_channel<Notifier> &m_channel;
    std::string           m_fqn;
    endpoint              m_remote;
    std::uint64_t         m_scheduler_key{detail::next_scheduler_key()};
    bool                  m_released = false;
    plexus::detail::move_only_function<void(std::span<const std::byte>)>      m_on_data;
    plexus::detail::move_only_function<void()>                               m_on_closed;
    plexus::detail::move_only_function<void(io_error)>                       m_on_error;
    plexus::detail::move_only_function<void(const detail::drop_event &)>      m_on_drop;
    plexus::detail::move_only_function<void(wire::close_cause)>              m_on_protocol_close;
};

// The shared-memory mux member: the SECOND same-host (local-tier) transport, joining
// AF_UNIX through the multiplexer's multi-member-per-tier seam. It satisfies mux_member
// -- channel_type + the "shm" scheme + the local tier -- so the core multiplexer routes
// a same-host "shm" endpoint to it generically. It opts into the per-candidate same-host
// fast-path flag (mux_prefers_shm) so the composition's preference hook can find it.
//
// A dial(ep) demand-acquires the ring for the topic (ep.address is the fqn) and, on a
// created/attached verdict, mints an shm_byte_channel over the live ring and fires
// on_dialed; a failed acquire fires on_dial_failed (the mux's preference hook prefers
// this member only when can_acquire(ep) is true, so a routed dial here is expected to
// succeed -- the hook gates the fallback to AF_UNIX). listen(ep) is the creator side:
// it acquires (minting the ring) and announces the accepted channel.
//
// It owns the registry (the sole ring-lifecycle owner) and borrows the broker by
// reference. Templated on the broker + notifier seams so this core header pulls no
// POSIX/asio dependency; the asio composition supplies the posix broker + the reactor
// bridge notifier (and the binder that constructs the bridge over the ring word).
template <typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_mux_member
{
public:
    using channel_type   = shm_byte_channel<Broker, Notifier>;
    using registry_type  = shm_topic_registry<Broker, Notifier>;
    using notifier_binder = typename registry_type::notifier_binder;

    static constexpr std::array<std::string_view, 1> mux_schemes{"shm"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::local;
    static constexpr bool mux_prefers_shm = true;

    // The binder constructs each ring's notifier over the in-region generation word
    // (a reactor bridge captures the executor; the default emplaces a default-built
    // notifier, the stub/unit seam). Required-with-default: a node wiring the reactor
    // bridge supplies one, a default-constructible notifier needs none.
    shm_mux_member(Broker &broker, reliability rel, congestion cong,
                   notifier_binder bind_notifier = registry_type::default_notifier_binder()) noexcept
        : m_registry(broker, rel, cong, std::move(bind_notifier))
    {
    }

    shm_mux_member(const shm_mux_member &) = delete;
    shm_mux_member &operator=(const shm_mux_member &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb) { m_on_error = std::move(cb); }

    // The creator side: acquire (minting) the ring for the topic and announce the
    // accepted channel over it.
    void listen(const endpoint &ep)
    {
        auto ch = open(ep);
        if(ch && m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // The dialer side: acquire the ring and hand the live channel up, carrying the
    // dialed endpoint back so the engine correlates the completion to its slot.
    void dial(const endpoint &ep)
    {
        auto ch = open(ep);
        if(!ch)
            return report_dial_fail(ep, io_error::connection_refused);
        if(m_on_dialed)
            m_on_dialed(std::move(ch), ep);
    }

    void close() {}

    // Whether the preference hook should prefer this member for ep: the ring acquire
    // for ep.address must succeed. A successful probe LEAVES the ring acquired (the
    // refcount bumped) so the immediately-following dial reuses the SAME ring with no
    // teardown/re-attach churn; a probe that cannot acquire releases nothing (it never
    // bumped). The hook calls this to gate the SHM-vs-AF_UNIX choice at dial time -- the
    // runtime ring-acquire success the positional order cannot express. If the hook
    // declines this member after a successful probe, abandon() drops the held bump.
    [[nodiscard]] bool can_acquire(const endpoint &ep)
    {
        const resolved_geometry g = resolve(ep.address, ring_direction::request);
        return m_registry.acquire(ep.address, ring_direction::request, g.max_payload, g.mode,
                                  g.consumer_capacity) != acquire_result::failed;
    }

    // Drop a held probe bump the dial did not consume (the hook probed shm but chose the
    // stream fallback for a co-tier reason). Keeps the refcount honest.
    void abandon(const endpoint &ep) { m_registry.release(ep.address, ring_direction::request); }

    registry_type &registry() noexcept { return m_registry; }

    // The producer-side same-host provisioning channel: the declaring publisher records
    // the effective per-message size and resolved geometry for its topic here, keyed by
    // fqn, BEFORE the dial/listen that mints the ring. It is a LOCAL value communicated
    // to the ring through the config block, NOT the wire — a remote peer cannot inject a
    // geometry. An fqn with no entry (a subscriber-only attach, or an unprovisioned
    // topic) resolves to the default ring, exactly as before.
    void set_topic_geometry(const std::string &fqn, std::size_t effective_bytes, shm_geometry geom)
    {
        m_geometry.insert_or_assign(
            fqn, provisioned{static_cast<std::uint32_t>(effective_bytes), geom.mode, geom.max_consumers});
    }

    // The resolved mode + the capped ring's per-message slot capacity for an fqn, read
    // by the publish fan-out's per-message route (the wire_fallback size check). The cap
    // is the bounded ring's slot stride (round_up_8 of the provisioned payload), the same
    // value ring_geometry_for hands the live acquire — a message at or under it fits the
    // ring, a larger one reroutes over the wire. An unprovisioned fqn resolves to the
    // reliable_preserving default (so the forwarder's mode gate never enters the
    // per-message branch for it), mirroring resolve()'s default.
    struct resolved_topic_geometry
    {
        ring_geometry_mode mode          = ring_geometry_mode::reliable_preserving;
        std::uint64_t      slot_capacity = 0;
    };

    [[nodiscard]] resolved_topic_geometry resolved_geometry_for(const std::string &fqn) const
    {
        auto it = m_geometry.find(fqn);
        if(it == m_geometry.end())
            return {};
        const provisioned &p = it->second;
        const ring_geometry geom = ring_geometry_for(p.max_payload, p.mode, p.consumer_capacity);
        return {p.mode, geom.slot_capacity};
    }

    // Apply the node-level per-ring slab ceiling to the owned registry (forwarded once
    // by the node owner at construction).
    void set_max_ring_slab_bytes(std::uint64_t bytes) noexcept
    {
        m_registry.set_max_ring_slab_bytes(bytes);
    }

private:
    // The per-fqn provisioned geometry: the publisher's effective payload width, the
    // declared (or node/shipped-default) mode, and the declared consumer capacity (0 =
    // the shipped floor). All producer-side, never wire-advertised.
    struct provisioned
    {
        std::uint32_t      max_payload        = 0;
        ring_geometry_mode mode               = ring_geometry_mode::reliable_preserving;
        std::uint32_t      consumer_capacity  = 0;
    };

    // The resolved acquire arguments for one (fqn, direction): the request direction
    // (publisher) sizes the ring from the provisioned width; the response direction
    // (subscriber) passes 0 and falls back to the default ring (the consumer rescues
    // itself). upgrade_ring_max_payload is the direction authority.
    struct resolved_geometry
    {
        std::uint32_t      max_payload;
        ring_geometry_mode mode;
        std::uint32_t      consumer_capacity;
    };

    [[nodiscard]] resolved_geometry resolve(const std::string &fqn, ring_direction direction) const
    {
        auto it = m_geometry.find(fqn);
        if(it == m_geometry.end())
            return {upgrade_ring_max_payload(direction, 0), ring_geometry_mode::reliable_preserving, 0};
        const provisioned &p = it->second;
        return {upgrade_ring_max_payload(direction, p.max_payload), p.mode, p.consumer_capacity};
    }

    // Mint a channel over the ring for ep, acquiring it first unless a prior can_acquire
    // probe already holds it (channel_for is then already non-null -- reuse that held
    // reference, so the refcount stays at one held by the minted channel). Returns nullptr
    // on a broker failure. ep.address is the fqn the deterministic region name derives from.
    std::unique_ptr<channel_type> open(const endpoint &ep)
    {
        shm_channel<Notifier> *ch = m_registry.channel_for(ep.address, ring_direction::request);
        if(ch == nullptr) // no probe held it: acquire fresh
        {
            const resolved_geometry g = resolve(ep.address, ring_direction::request);
            if(m_registry.acquire(ep.address, ring_direction::request, g.max_payload, g.mode,
                                  g.consumer_capacity) == acquire_result::failed)
                return nullptr;
            ch = m_registry.channel_for(ep.address, ring_direction::request);
        }
        if(ch == nullptr)
            return nullptr;
        return std::make_unique<channel_type>(m_registry, *ch, ep.address, ep);
    }

    void report_dial_fail(const endpoint &ep, io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    registry_type m_registry;
    std::unordered_map<std::string, provisioned> m_geometry;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)>                  m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const endpoint &, io_error)>                      m_on_dial_failed;
    plexus::detail::move_only_function<void(io_error)>                                        m_on_error;
};

// The same-host preference hook: prefer the shared-memory candidate when the pair is
// SHM-eligible AND the ring acquire succeeds, else fall back to the AF_UNIX candidate.
// This is the FIRST case where a tier (local) resolves to >1 candidate, so the choice
// is load-bearing -- and it is NOT a pure positional order: it depends on the RUNTIME
// ring-acquire success (a forced broker failure must fall back to the stream). The hook
// reads the widened per-candidate descriptor's shm_eligible flag to find the fast-path
// candidate, then consults the injected can_acquire probe (the SHM member's, captured by
// move-only function so the erased hook stays decoupled from the concrete member type --
// the seed constraint). A non-eligible endpoint, or one the probe declines, routes to
// the first non-SHM candidate (the stream fallback). The probe captures the member by
// reference; the member outlives the mux (the borrow discipline the multiplexer documents).
template <typename Member>
[[nodiscard]] inline io::selection_hook prefer_shm_hook(Member &member)
{
    plexus::detail::move_only_function<bool(const endpoint &)> can_acquire =
        [&member](const endpoint &ep) { return member.can_acquire(ep); };
    return [probe = std::move(can_acquire)](const endpoint &ep,
                                            std::span<const io::mux_candidate> candidates) mutable
               -> std::size_t {
        std::size_t fallback = candidates.front().index; // the first candidate (stream)
        bool        have_fallback = false;
        for(const io::mux_candidate &c : candidates)
        {
            if(c.shm_eligible)
            {
                if(probe(ep))
                    return c.index; // SHM-eligible AND the ring acquired: take the fast path
                continue;           // eligible but the acquire failed: keep scanning for a stream
            }
            if(!have_fallback)
            {
                fallback      = c.index; // the first non-SHM candidate is the stream fallback
                have_fallback = true;
            }
        }
        return fallback;
    };
}

}

#endif
