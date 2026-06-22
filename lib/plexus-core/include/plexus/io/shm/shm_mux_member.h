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
#include "plexus/io/shm/shm_byte_channel.h"
#include "plexus/io/shm/shm_topic_registry.h"
#include "plexus/io/shm/shm_preference_hook.h"
#include "plexus/io/shm/detail/shm_mux_acquire.h"

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

// The same-host upgrade decision: given THIS end's same_host verdict and the topic's own
// dispatch hint, whether to attempt the shared-memory ring acquire. A plain function pointer
// keeps the carrying surface trivially copyable (the injected-predicate style, not an allocating
// policy registry). It can only DECLINE the upgrade — the same_host gate inside the default still
// holds. Owned by the shm transport (the user composes that transport), not by node_options.
using upgrade_policy_fn = bool (*)(bool same_host, dispatch_hint own_hint);

// The shipped default: the bilateral, consumer-sovereign same-host auto-upgrade.
[[nodiscard]] inline bool default_upgrade_policy(bool same_host, dispatch_hint own_hint) noexcept
{
    return attempt_shm_upgrade(same_host, own_hint);
}

// over-limit: one cohesive mux_member contract; the dial/listen/can_acquire/companion verbs
// all drive the OWNED registry (the sole ring-lifecycle owner) + the per-fqn geometry map, so
// splitting the surface scatters that shared ownership state (the channel, the consumer, the
// preference hook, and the acquire/resolve glue are extracted to sibling + detail/ headers).
//
// The shared-memory mux member: the SECOND same-host (local-tier) transport, joining AF_UNIX
// through the multiplexer's multi-member-per-tier seam. It satisfies mux_member (channel_type +
// the "shm" scheme + the local tier) and opts into mux_prefers_shm so the preference hook finds
// it. dial(ep) demand-acquires the ring for the topic (ep.address is the fqn) and mints an
// shm_byte_channel on success / fires on_dial_failed otherwise; listen(ep) is the creator side.
// It OWNS the registry (the sole ring-lifecycle owner) and borrows the broker by reference.
// Templated on the broker + notifier seams so this core header pulls no POSIX/asio dependency;
// the acquire/open glue is extracted to detail/shm_mux_acquire.h, the channel to
// shm_byte_channel.h, the preference hook to shm_preference_hook.h.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_mux_member
{
public:
    using channel_type    = shm_byte_channel<Broker, Notifier>;
    using registry_type   = shm_topic_registry<Broker, Notifier>;
    using notifier_binder = typename registry_type::notifier_binder;

    static constexpr std::array<std::string_view, 1> mux_schemes{"shm"};
    static constexpr io::transport_kind              mux_tier        = io::transport_kind::local;
    static constexpr bool                            mux_prefers_shm = true;

    // bind_notifier constructs each ring's notifier over the in-region generation word
    // (required-with-default: a default-constructible notifier needs none). region_ns is the
    // static shm-region namespace (EMPTY shares rings by topic; a distinct namespace isolates
    // this application's same-host shm from unrelated apps).
    shm_mux_member(Broker &broker, reliability rel, congestion cong,
                   notifier_binder bind_notifier = registry_type::default_notifier_binder(),
                   std::string     region_ns     = {}) noexcept
            : m_registry(broker, rel, cong, std::move(bind_notifier), k_max_ring_slab_bytes,
                         std::move(region_ns))
    {
    }

    shm_mux_member(const shm_mux_member &)            = delete;
    shm_mux_member &operator=(const shm_mux_member &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>,
                                                           const endpoint &)>
                           cb)
    {
        m_on_dialed = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    // The creator side: acquire (minting) the ring for the topic and announce the
    // accepted channel over it.
    void listen(const endpoint &ep)
    {
        auto ch = detail::mux_open(*this, ep);
        if(ch && m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // The dialer side: acquire the ring and hand the live channel up, carrying the
    // dialed endpoint back so the engine correlates the completion to its slot.
    void dial(const endpoint &ep)
    {
        auto ch = detail::mux_open(*this, ep);
        if(!ch)
            return report_dial_fail(ep, io_error::connection_refused);
        if(m_on_dialed)
            m_on_dialed(std::move(ch), ep);
    }

    void close() {}

    // Whether the preference hook should prefer this member for ep: the ring acquire for
    // ep.address must succeed. A successful probe LEAVES the ring acquired so the following
    // dial reuses the SAME ring with no churn; if the hook then declines, abandon() drops the
    // bump. A wire_fallback topic declines OUTRIGHT (no acquire): its recorded session channel
    // MUST be the wire so an over-cap message always has a reliable fallback (the capped ring
    // stays a small-message companion fast path, not the sole channel).
    [[nodiscard]] bool can_acquire(const endpoint &ep)
    {
        const auto g = detail::mux_resolve(*this, ep.address, ring_direction::request);
        if(g.mode == ring_geometry_mode::wire_fallback)
            return false;
        return m_registry.acquire(ep.address, ring_direction::request, g.max_payload, g.mode,
                                  g.consumer_capacity) != acquire_result::failed;
    }

    // Drop a held probe bump the dial did not consume (the hook probed shm but chose the
    // stream fallback for a co-tier reason). Keeps the refcount honest.
    void abandon(const endpoint &ep) { m_registry.release(ep.address, ring_direction::request); }

    // Mint a per-topic COMPANION ring channel for the same-host upgrade coordinator: the
    // additive fast path a co-host (peer, topic) pair runs ALONGSIDE its wire session. Unlike
    // can_acquire it mints for ANY provisioned mode (the wire stays the recorded fail-safe).
    // The returned channel holds the refcount and releases it on destruction.
    [[nodiscard]] std::unique_ptr<channel_type> mint_companion(const std::string &fqn)
    {
        // join_live, not reclaim_stale: the co-host subscriber may have minted this same
        // companion ring first (its receive lane), so a clobbering unlink would split the pair
        // onto two rings. Attach-first when it exists, mint when it does not.
        return detail::mux_open(*this, endpoint{"shm", fqn}, acquire_mode::join_live);
    }

    // The receive half of the companion model: attach the request-direction ring as a CONSUMER
    // for a co-host subscriber (sharing the ONE ring the publisher writes) and install on_frame
    // as the entry's consumer sink (the armed notifier delivers posted on the node executor; an
    // already-published message is delivered immediately so a late attach loses nothing).
    // Returns an RAII handle whose destruction clears the sink and releases the ring (1->0
    // unmaps). nullptr on a broker failure (the coordinator then keeps the wire receive path).
    // The framed bytes are header-on, so the caller feeds them into the SAME receive entry the
    // wire session feeds.
    [[nodiscard]] std::unique_ptr<shm_companion_consumer<Broker, Notifier>> mint_receive_companion(
            const std::string                                                   &fqn,
            plexus::detail::move_only_function<void(std::span<const std::byte>)> on_frame)
    {
        const auto g = detail::mux_resolve(*this, fqn, ring_direction::request);
        if(m_registry.acquire(fqn, ring_direction::request, g.max_payload, g.mode,
                              g.consumer_capacity,
                              acquire_mode::join_live) == acquire_result::failed)
            return nullptr;
        auto handle = std::make_unique<shm_companion_consumer<Broker, Notifier>>(m_registry, fqn);
        m_registry.set_consumer_sink(
                fqn, ring_direction::request,
                [cb = std::move(on_frame)](::plexus::wire_bytes<shm_slot_owner> wb) mutable
                { cb(std::span<const std::byte>{wb.data(), wb.size()}); });
        return handle;
    }

    registry_type &registry() noexcept { return m_registry; }

    // The producer-side same-host provisioning channel: the declaring publisher records the
    // effective per-message size + resolved geometry for its topic here, BEFORE the dial/listen
    // that mints the ring. A LOCAL value (never the wire — a remote peer cannot inject a
    // geometry). An fqn with no entry resolves to the default ring.
    void set_topic_geometry(const std::string &fqn, std::size_t effective_bytes, shm_geometry geom)
    {
        m_geometry.insert_or_assign(
                fqn,
                detail::shm_provisioned{static_cast<std::uint32_t>(effective_bytes), geom.mode,
                                        geom.max_consumers});
    }

    // The resolved mode + the capped ring's per-message slot capacity for an fqn, read by the
    // publish fan-out's per-message wire_fallback size check (a message at or under the slot
    // stride fits the ring, a larger one reroutes over the wire). An unprovisioned fqn resolves
    // to the reliable_preserving default.
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
        const auto         &p    = it->second;
        const ring_geometry geom = ring_geometry_for(p.max_payload, p.mode, p.consumer_capacity);
        return {p.mode, geom.slot_capacity};
    }

    // Apply the per-ring slab ceiling to the owned registry. The registry already defaults to
    // k_max_ring_slab_bytes at construction (the shipped ceiling), so a caller need only invoke
    // this to RAISE it for a larger reliable ring.
    void set_max_ring_slab_bytes(std::uint64_t bytes) noexcept
    {
        m_registry.set_max_ring_slab_bytes(bytes);
    }

    // The transport-default same-host ring geometry a publisher with no per-topic override resolves
    // against (the {0, reliable_preserving} shipped default: max_consumers 0 resolves to the
    // capacity floor, the safe reliable mode). Sourced by the node's declare/subscribe path so the
    // default lives on the transport that owns the rings, not on node_options.
    [[nodiscard]] shm_geometry default_geometry() const noexcept { return m_default_geometry; }

    // The consumer-sovereign same-host upgrade policy the medium coordinator consults on a co-host
    // demand edge. The default engages the upgrade out of the box; a deployment supplies a stricter
    // predicate (e.g. one always returning false) to disable it. Owned here, never on node_options.
    void set_upgrade_policy(upgrade_policy_fn policy) noexcept { m_upgrade_policy = policy; }
    [[nodiscard]] upgrade_policy_fn upgrade_policy() const noexcept { return m_upgrade_policy; }

private:
    template<typename M>
    friend auto detail::mux_open(M &, const io::endpoint &, shm::acquire_mode)
            -> std::unique_ptr<typename M::channel_type>;
    template<typename M>
    friend detail::shm_resolved_geometry detail::mux_resolve(const M &, const std::string &,
                                                             shm::ring_direction);

    void report_dial_fail(const endpoint &ep, io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    registry_type                                                           m_registry;
    std::unordered_map<std::string, detail::shm_provisioned>                m_geometry;
    shm_geometry      m_default_geometry{};
    upgrade_policy_fn m_upgrade_policy{&default_upgrade_policy};
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)>
                                                                         m_on_dialed;
    plexus::detail::move_only_function<void(const endpoint &, io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io_error)>                   m_on_error;
};

}

#endif
