#ifndef HPP_GUARD_PLEXUS_SHM_SHM_MUX_MEMBER_H
#define HPP_GUARD_PLEXUS_SHM_SHM_MUX_MEMBER_H

#include "plexus/shm/region_broker_concept.h"
#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/shm_selection.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_byte_channel.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/shm_preference_hook.h"
#include "plexus/shm/detail/shm_mux_acquire.h"

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
#include "plexus/wire/close_cause.h"
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

namespace plexus::shm {

// Whether THIS end attempts the shared-memory ring acquire for a (peer, topic). A plain
// function pointer keeps the carrying surface trivially copyable; it can only DECLINE the
// upgrade (the same_host gate inside the default still holds).
using upgrade_policy_fn = bool (*)(bool same_host, io::dispatch_hint own_hint);

inline bool default_upgrade_policy(bool same_host, io::dispatch_hint own_hint) noexcept
{
    return attempt_shm_upgrade(same_host, own_hint);
}

// The shared-memory mux member: the SECOND same-host (local-tier) transport, joining AF_UNIX
// through the multiplexer's multi-member-per-tier seam. dial(ep) demand-acquires the ring for
// the topic (ep.address is the fqn) and mints an shm_byte_channel on success / fires
// on_dial_failed otherwise; listen(ep) is the creator side. It OWNS the registry and borrows
// the broker by reference. Templated on the broker + notifier seams so this core header pulls
// no POSIX/asio dependency.
template<typename Broker, typename Notifier>
    requires region_broker<Broker> && notifier<Notifier>
class shm_mux_member
{
public:
    using channel_type    = shm_byte_channel<Broker, Notifier>;
    using registry_type   = shm_topic_registry<Broker, Notifier>;
    using notifier_binder = typename registry_type::notifier_binder;

    static constexpr std::array<std::string_view, 1> mux_schemes{"shm"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::local;
    static constexpr bool mux_prefers_local_fast = true;

    // region_ns is the static shm-region namespace (EMPTY shares rings by topic; a distinct
    // namespace isolates this application's same-host shm from unrelated apps).
    shm_mux_member(Broker &broker, io::reliability rel, io::congestion cong, notifier_binder bind_notifier = registry_type::default_notifier_binder(),
                   std::string region_ns = {}) noexcept
            : m_registry(broker, rel, cong, std::move(bind_notifier), k_max_ring_slab_bytes, std::move(region_ns))
    {
    }

    shm_mux_member(const shm_mux_member &)            = delete;
    shm_mux_member &operator=(const shm_mux_member &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const io::endpoint &)> cb)
    {
        m_on_dialed = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void listen(const io::endpoint &ep)
    {
        auto ch = detail::mux_open(*this, ep);
        if(ch && m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // Carries the dialed io::endpoint back so the engine correlates the completion to its slot.
    void dial(const io::endpoint &ep)
    {
        auto ch = detail::mux_open(*this, ep);
        if(!ch)
            return report_dial_fail(ep, io::io_error::connection_refused);
        if(m_on_dialed)
            m_on_dialed(std::move(ch), ep);
    }

    void close()
    {
    }

    // A successful probe LEAVES the ring acquired so the following dial reuses the SAME ring with
    // no churn; if the hook then declines, abandon() drops the bump. A wire_fallback topic
    // declines OUTRIGHT (no acquire): its recorded session channel MUST be the wire so an over-cap
    // message always has a reliable fallback.
    bool can_acquire(const io::endpoint &ep)
    {
        const auto g = detail::mux_resolve(*this, ep.address, ring_direction::request);
        if(g.mode == ring_geometry_mode::wire_fallback)
            return false;
        return m_registry.acquire(ep.address, ring_direction::request, g.max_payload, g.mode, g.consumer_capacity) != acquire_result::failed;
    }

    // Drop a held probe bump the dial did not consume, keeping the refcount honest.
    void abandon(const io::endpoint &ep)
    {
        m_registry.release(ep.address, ring_direction::request);
    }

    // The additive fast path a co-host (peer, topic) pair runs ALONGSIDE its wire session. Unlike
    // can_acquire it mints for ANY provisioned mode (the wire stays the recorded fail-safe). The
    // returned channel holds the refcount and releases it on destruction.
    std::unique_ptr<channel_type> mint_companion(const std::string &fqn)
    {
        // join_live: the co-host subscriber may have minted this companion ring first, so a
        // clobbering unlink would split the pair onto two rings.
        return detail::mux_open(*this, io::endpoint{"shm", fqn}, acquire_mode::join_live);
    }

    // The receive half of the companion model: attach the request-direction ring as a CONSUMER
    // for a co-host subscriber (sharing the ONE ring the publisher writes) and install on_frame
    // as the entry's consumer sink. Returns an RAII handle whose destruction clears the sink and
    // releases the ring (1->0 unmaps); nullptr on a broker failure. The framed bytes are
    // header-on, so the caller feeds them into the SAME receive entry the wire session feeds.
    std::unique_ptr<shm_companion_consumer<Broker, Notifier>> mint_receive_companion(const std::string &fqn,
                                                                                     plexus::detail::move_only_function<void(std::span<const std::byte>)> on_frame)
    {
        const auto g = detail::mux_resolve(*this, fqn, ring_direction::request);
        if(m_registry.acquire(fqn, ring_direction::request, g.max_payload, g.mode, g.consumer_capacity, acquire_mode::join_live) == acquire_result::failed)
            return nullptr;
        auto handle = std::make_unique<shm_companion_consumer<Broker, Notifier>>(m_registry, fqn);
        m_registry.set_consumer_sink(fqn, ring_direction::request,
                                     [cb = std::move(on_frame)](::plexus::wire_bytes<shm_slot_owner> wb) mutable { cb(std::span<const std::byte>{wb.data(), wb.size()}); });
        return handle;
    }

    registry_type &registry() noexcept
    {
        return m_registry;
    }

    // The declaring publisher records its topic's effective per-message size + geometry here,
    // BEFORE the dial/listen that mints the ring. A LOCAL value (a remote peer cannot inject a
    // geometry); an fqn with no entry resolves to the default ring.
    void set_topic_geometry(const std::string &fqn, std::size_t effective_bytes, shm_geometry geom)
    {
        m_geometry.insert_or_assign(fqn, detail::shm_provisioned{static_cast<std::uint32_t>(effective_bytes), geom.mode, geom.max_consumers});
    }

    // Read by the publish fan-out's per-message wire_fallback size check. An unprovisioned fqn
    // resolves to the reliable_preserving default.
    struct resolved_topic_geometry
    {
        ring_geometry_mode mode     = ring_geometry_mode::reliable_preserving;
        std::uint64_t slot_capacity = 0;
    };

    resolved_topic_geometry resolved_geometry_for(const std::string &fqn) const
    {
        auto it = m_geometry.find(fqn);
        if(it == m_geometry.end())
            return {};
        const auto &p            = it->second;
        const ring_geometry geom = ring_geometry_for(p.max_payload, p.mode, p.consumer_capacity);
        return {p.mode, geom.slot_capacity};
    }

    // The runtime-acquire selection hook this member contributes to a multi-candidate local tier:
    // prefer the ring when it acquires, else fall back to the stream. The companion seam the node
    // drives generically, so the node never names an shm selection symbol.
    io::selection_hook companion_selection_hook()
    {
        return prefer_upgradeable_hook(*this);
    }

    // The per-message route a co-host (peer, topic) pair runs: a message rides the ring when the
    // resolved mode and capacity route it to shm. Captures the resolved geometry by value, so the
    // route is fixed at mint time.
    plexus::detail::move_only_function<bool(std::size_t)> companion_route(const std::string &fqn)
    {
        const auto g = resolved_geometry_for(fqn);
        return [mode = g.mode, cap = g.slot_capacity](std::size_t bytes) { return route_message_medium(mode, bytes, cap) == same_host_medium::shm; };
    }

    // The registry already defaults to k_max_ring_slab_bytes, so a caller need only invoke this
    // to RAISE the ceiling for a larger reliable ring.
    void set_max_ring_slab_bytes(std::uint64_t bytes) noexcept
    {
        m_registry.set_max_ring_slab_bytes(bytes);
    }

    // The transport-default ring geometry a publisher with no per-topic override resolves against;
    // the default lives on the transport that owns the rings, not on node_options.
    shm_geometry default_geometry() const noexcept
    {
        return m_default_geometry;
    }

    // Recover a per-topic override the api carries as an opaque pointer at the declare seam (null =
    // none, the default). A LOCAL value the producer supplied for this synchronous declare; the
    // concrete type stays member-side so the generic api names no shm geometry type.
    shm_geometry geometry_from(const void *override_or_null) const noexcept
    {
        return override_or_null != nullptr ? *static_cast<const shm_geometry *>(override_or_null) : m_default_geometry;
    }

    // The upgrade policy the medium coordinator consults on a co-host demand edge. The default
    // engages the upgrade; a deployment supplies a stricter predicate to disable it.
    void set_upgrade_policy(upgrade_policy_fn policy) noexcept
    {
        m_upgrade_policy = policy;
    }
    upgrade_policy_fn upgrade_policy() const noexcept
    {
        return m_upgrade_policy;
    }

private:
    template<typename M>
    friend auto detail::mux_open(M &, const io::endpoint &, shm::acquire_mode) -> std::unique_ptr<typename M::channel_type>;
    template<typename M>
    friend detail::shm_resolved_geometry detail::mux_resolve(const M &, const std::string &, shm::ring_direction);

    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    registry_type m_registry;
    std::unordered_map<std::string, detail::shm_provisioned> m_geometry;
    shm_geometry m_default_geometry{};
    upgrade_policy_fn m_upgrade_policy{&default_upgrade_policy};
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

#endif
