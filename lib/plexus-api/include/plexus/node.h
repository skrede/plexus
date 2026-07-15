#ifndef HPP_GUARD_PLEXUS_NODE_H
#define HPP_GUARD_PLEXUS_NODE_H

#include "plexus/recorder.h"
#include "plexus/node_options.h"
#include "plexus/typed_publisher_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/node_name.h"
#include "plexus/io/endpoint_seam.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/observer.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/intra_node_transport.h"
#include "plexus/io/multiplexing_transport.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/process_loopback_channel.h"

#include "plexus/log/logger.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/match/key_pattern.h"

#include "plexus/muxify.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/policy.h"
#include "plexus/transport_priority.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/fail_closed.h"
#include "plexus/detail/topic_sweep.h"
#include "plexus/detail/address_parse.h"
#include "plexus/detail/node_self_route.h"
#include "plexus/detail/node_self_carrier.h"
#include "plexus/detail/node_upgrade_wiring.h"
#include "plexus/detail/function_traits.h"
#include "plexus/detail/node_internals.h"

#include <span>
#include <tuple>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <charconv>
#include <optional>
#include <algorithm>
#include <functional>
#include <type_traits>
#include <string_view>

namespace plexus {

namespace detail {

template<typename Policy, typename... Transports>
using node_engine_policy = std::conditional_t<sizeof...(Transports) == 1, Policy, muxify<Policy>>;

// True when the transport pack carries an intra_node_transport<Policy> — the node then self-attaches
// its loopback channel. The pure single-transport case AND a composed intra-node+network node both
// match; only a pack with no intra-node member leaves the self-route idle.
template<typename Policy, typename... Transports>
constexpr bool pack_has_intra_node = (std::is_same_v<Transports, io::intra_node_transport<Policy>> || ...);

// Keys on the name-free geometry surface (the same probe for_each_shm_member uses), so the predicate
// names no shm type: a member that provisions per-topic rings matches; a plain leaf does not.
template<typename M>
constexpr bool member_has_shm_ring = requires(M &m) {
    m.set_topic_geometry(std::string{}, std::size_t{}, m.default_geometry());
    m.geometry_from(static_cast<const void *>(nullptr));
};

// True when the pack carries a member with the shm-ring surface — the fallback self-carrier (a
// node-local shm self-ring) is available when no intra-node transport is present to serve self.
template<typename... Transports>
constexpr bool pack_has_shm_member = (member_has_shm_ring<Transports> || ...);

// The variadic ctor lets the node's member-init use ONE in-place construction expression for
// both glue kinds (this type ignores the leaves; the mux consumes them).
struct no_mux_glue
{
    no_mux_glue() = default;
    template<typename... Ignored>
    explicit no_mux_glue(Ignored &&...) noexcept
    {
    }
};

// A `::type` indirection rather than std::conditional_t, so the multiplexing_transport branch is
// never instantiated for a single transport (eager instantiation would fail mux_member for it).
template<bool Single, typename... Transports>
struct mux_selection
{
    using transport = io::multiplexing_transport<Transports...>;
    using glue      = io::multiplexing_transport<Transports...>;
};

template<typename... Transports>
struct mux_selection<true, Transports...>
{
    using transport = std::tuple_element_t<0, std::tuple<Transports...>>;
    using glue      = no_mux_glue;
};

template<typename... Transports>
using node_engine_transport = typename mux_selection<sizeof...(Transports) == 1, Transports...>::transport;

template<typename... Transports>
using node_mux_glue = typename mux_selection<sizeof...(Transports) == 1, Transports...>::glue;

// FNV-1a 64 over the name, run twice with distinct offset baselines, folding the two 8-byte
// digests into the 16-byte node_id. Deterministic and platform-stable: the same name yields the
// same identity.
inline node_id hash_node_id(std::string_view name) noexcept
{
    constexpr std::uint64_t k_prime    = 1099511628211ull;
    constexpr std::uint64_t k_basis_lo = 1469598103934665603ull;
    constexpr std::uint64_t k_basis_hi = 0x9e3779b97f4a7c15ull;
    std::uint64_t lo                   = k_basis_lo;
    std::uint64_t hi                   = k_basis_hi;
    for(char c : name)
    {
        const auto byte = static_cast<std::uint64_t>(static_cast<unsigned char>(c));
        lo              = (lo ^ byte) * k_prime;
        hi              = (hi ^ byte) * k_prime;
    }
    node_id id{};
    for(int i = 0; i < 8; ++i)
    {
        id[i]     = static_cast<std::byte>((lo >> (8 * i)) & 0xff);
        id[8 + i] = static_cast<std::byte>((hi >> (8 * i)) & 0xff);
    }
    return id;
}

}

// Equal to the id() of a node constructed with the same name string.
inline node_id node_id_from_name(std::string_view name) noexcept
{
    return detail::hash_node_id(name);
}

struct value_logger_options;
struct no_projection;

template<typename Codec = void>
class publisher;

template<typename Codec = void>
class subscriber;

template<typename Codec, typename Projection = no_projection>
class value_logger;

template<typename T>
struct no_codec;

template<typename Sig = void, template<typename> class CReq = no_codec, template<typename> class CRes = CReq>
class caller;

template<typename Sig = void, template<typename> class CReq = no_codec, template<typename> class CRes = CReq>
class procedure;

// LIFETIME: the node BORROWS its executor, discovery, and transport leaves and OWNS only the
// engine and (for >1 transport) the multiplexing glue. Every engine callback is POSTED on the
// borrowed executor and captures `this`. The borrowed substrate MUST outlive the node, and the
// executor MUST be drained before the node is destroyed — the structural single-owner
// discipline. The node pins `this` into those callbacks, so it is non-copyable and non-movable.
template<typename Policy, typename... Transports>
    requires plexus::Policy<Policy> && (sizeof...(Transports) >= 1) &&
        (sizeof...(Transports) == 1 ? io::transport_backend<std::tuple_element_t<0, std::tuple<Transports..., void>>, Policy> : (io::mux_member<Transports> && ...))
class basic_node
{
    // The registration/retire seams are private; the endpoint handles reach them only as friends
    // (there is no public node.publish/subscribe factory).
    template<typename C>
    friend class publisher;
    template<typename C>
    friend class subscriber;
    template<typename C, typename Pr>
    friend class value_logger;
    template<typename S, template<typename> class Cq, template<typename> class Cs>
    friend class caller;
    template<typename S, template<typename> class Cq, template<typename> class Cs>
    friend class procedure;

public:
    using policy_type      = Policy;
    using executor_type    = typename Policy::executor_type;
    using engine_policy    = detail::node_engine_policy<Policy, Transports...>;
    using engine_transport = detail::node_engine_transport<Transports...>;
    using engine_type      = io::routing_engine<engine_policy, engine_transport>;
    using engine_channel   = typename engine_type::channel_type;
    using transport_tuple  = std::tuple<Transports...>;

    // The self-route engages whenever the pack carries an intra_node_transport: the pure
    // single-transport node binds the loopback channel as channel_type (concrete, zero erasure,
    // object fast path); a composed intra-node+network node binds polymorphic_byte_channel, so the
    // self-channel is wrapped (erased, framed bytes lane only — no zero-copy self-lane there). Any
    // pack with no intra-node member leaves the self-route idle.
    static constexpr bool k_has_self_loopback = detail::pack_has_intra_node<Policy, Transports...>;

    // Single-transport: the engine's channel IS the concrete loopback channel, attached directly.
    // Multi-transport: the engine's channel is the erased polymorphic_byte_channel, so the concrete
    // self-channel is reference-adapter wrapped to satisfy the registry's reference_wrapper.
    static constexpr bool k_self_route_erased = k_has_self_loopback && !std::is_same_v<engine_channel, io::process_loopback_channel<Policy>>;

    // The fallback self-carrier: when no intra-node transport serves self but the pack has an shm
    // member, a same-node publish self-delivers over a node-local shm self-ring (the framed bytes
    // lane). Mutually exclusive with the intra-node self-route — at most one carrier per node.
    static constexpr bool k_has_shm_self_carrier = !k_has_self_loopback && detail::pack_has_shm_member<Transports...>;
    static_assert(!(k_has_self_loopback && k_has_shm_self_carrier), "a node installs at most one self-route carrier (intra-node OR shm self-ring), never both");

    // The node_id is taken verbatim: plexus compares the identity, never mints or interprets it.
    basic_node(executor_type executor, discovery::discovery &disc, const plexus::node_id &id, Transports &...transports, const node_options &opts)
            : m_id(id)
            , m_executor(executor)
            , m_disc(disc)
            , m_logger(resolve_logger(opts))
            , m_service_name(opts.name.empty() ? io::node_name_of(id) : opts.name)
            , m_max_message_bytes(opts.max_message_bytes)
            , m_wire_crypto_position(opts.wire.position)
            // The leaves are BORROWED three ways and never consumed (mux glue, resolve_hook's
            // by-ref capture, engine_leaf's re-read), so a future edit must NOT move into the mux
            // ctor — that would invalidate the subsequent engine_leaf / resolve_hook reads.
            , m_glue(transports..., io::transport_selector{}, detail::resolve_hook(transports...))
            , m_leaf(engine_leaf(transports...))
            , m_self_channel(executor, m_service_name)
            , m_engine(m_leaf, executor, make_fsm_cfg(id, opts), opts.handshake_timeout, opts.reconnect, opts.redial_seed, resolve_logger(opts), opts.dial_eagerly,
                       opts.max_message_bytes, opts.liveliness)
    {
        // The routes are installed and the card advertised synchronously in the ctor turn, before
        // any session is built (the set-before-listen contract).
        m_engine.on_message_route([this](std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info) { dispatch_message(fqn, bytes, info); });
        m_engine.on_object_route([this](std::string_view fqn, const io::object_carrier &carrier) { dispatch_object(fqn, carrier); });
        m_engine.add_observer(m_peer_watch);
        advertise_card();
        m_disc.browse([this](const discovery::service_info &peer) { note_from_card(peer); });
        m_disc.on_withdrawn([this](const discovery::service_info &peer) { forget_from_card(peer); });
        install_upgrade_coordinator();
        install_self_loopback();
        m_engine.capture().set_default(opts.capture.to_rule());
        m_engine.post_participant({io::participant_edge::created, m_id});
    }

    basic_node(executor_type executor, discovery::discovery &disc, std::string_view name, Transports &...transports, const node_options &opts)
            : basic_node(executor, disc, detail::hash_node_id(name), transports..., opts)
    {
    }

    basic_node(const basic_node &)            = delete;
    basic_node &operator=(const basic_node &) = delete;
    basic_node(basic_node &&)                 = delete;
    basic_node &operator=(basic_node &&)      = delete;

    // The peer watcher is deregistered FIRST so the teardown edge's observer snapshot (captured in
    // place of the engine, which is destroyed before the executor drains the closure) holds only
    // externally-owned observers that outlive the drain. A dtor must not throw — the post is wrapped.
    ~basic_node()
    {
#if defined(__cpp_exceptions)
        try
        {
            m_engine.remove_observer(m_peer_watch);
            m_engine.post_participant_teardown({io::participant_edge::destroyed, m_id});
        }
        catch(...)
        {
        }
#else
        m_engine.remove_observer(m_peer_watch);
        m_engine.post_participant_teardown({io::participant_edge::destroyed, m_id});
#endif
    }

    // An advertised listen requires an EXPLICIT port ("host:port"): a port-0 auto-assign cannot be
    // advertised (the transport_backend concept exposes no bound-port accessor), so it binds the
    // engine but advertises no port key.
    void listen(const io::endpoint &ep)
    {
        m_engine.listen(ep);
        if(const auto port = plexus::detail::port_of(ep.address))
        {
            if(m_host.empty())
                m_host = plexus::detail::host_of(ep.address);
            m_listens.push_back({ep.scheme, *port});
            advertise_card();
        }
    }

    // A directly-dialed endpoint is not advertisable (no card, no listen key): forward to the engine
    // and nothing else. The peer's real id arrives in the handshake.
    void dial(const io::endpoint &ep)
    {
        m_engine.dial(ep);
    }

    const plexus::node_id &id() const noexcept
    {
        return m_id;
    }

    engine_type &router() noexcept
    {
        return m_engine;
    }
    const engine_type &router() const noexcept
    {
        return m_engine;
    }
    io::message_forwarder<engine_policy> &message_forwarder() noexcept
    {
        return m_engine.messages();
    }
    executor_type executor() const noexcept
    {
        return m_executor;
    }

    // Executor-affine: call only on the owning executor. Sweeps the awareness table with no lock
    // and no allocation, filling out to capacity and reporting overflow as a count plus a flag —
    // never abort, never evict (reject-and-count at the span boundary).
    graph::snapshot_result participants(std::span<graph::participant_record> out) const
    {
        std::size_t filled = 0;
        bool        truncated = false;
        m_engine.known().for_each([&](const plexus::node_id &id, const io::endpoint &ep) {
            if(filled == out.size())
            {
                truncated = true;
                return;
            }
            out[filled++] = graph::participant_record{id, graph::route{ep, std::nullopt},
                                                      graph::provenance{graph::observation::directly_observed, std::nullopt}};
        });
        return graph::snapshot_result{filled, truncated};
    }

    // Executor-affine: call only on the owning executor. One record per (participant, topic, role)
    // edge, so a topic with both a publisher and a subscriber yields both — these edges are the
    // substrate the counts and by-node views reduce over, which is why neither keeps an index of
    // its own. A filter admits only the topics its keyset intersects.
    graph::snapshot_result topics(std::span<graph::topic_record> out,
                                  const std::optional<match::key_pattern> &filter = std::nullopt) const
    {
        return detail::sweep_topics(m_engine.topic_table(), out,
                                    [&](const graph::topic_record &rec) { return detail::topic_in(rec.name, filter); });
    }

    // Executor-affine. Counted over the same edges topics() enumerates: a maintained counter would
    // be a second truth to keep coherent with the table.
    std::size_t count_publishers(std::string_view topic) const
    {
        return detail::count_topic_role(m_engine.topic_table(), topic, graph::topic_role::publisher);
    }

    std::size_t count_subscribers(std::string_view topic) const
    {
        return detail::count_topic_role(m_engine.topic_table(), topic, graph::topic_role::subscriber);
    }

    // Executor-affine. The same edges keyed by participant rather than by topic.
    graph::snapshot_result topics_published_by(const plexus::node_id &node, std::span<graph::topic_record> out) const
    {
        return topics_of(node, graph::topic_role::publisher, out);
    }

    graph::snapshot_result topics_subscribed_by(const plexus::node_id &node, std::span<graph::topic_record> out) const
    {
        return topics_of(node, graph::topic_role::subscriber, out);
    }

    // Topics whose type list is not the whole truth: a name clipped to its bound, or a distinct
    // type past the per-topic cap. Edges refused outright for want of room.
    std::size_t topic_truncations() const noexcept
    {
        return m_engine.topic_table().truncations();
    }

    std::size_t topics_dropped() const noexcept
    {
        return m_engine.topic_table().dropped();
    }

    // Object-lane deliveries dropped at the demux on a type-witness mismatch.
    std::size_t object_dispatch_mismatch() const noexcept
    {
        return m_object_dispatch_mismatch;
    }

    // The codec FAMILY is spelled explicitly (Family is NON-defaulted, sidestepping the MSVC
    // template-template defaulting hazard); the signature/value type is DEDUCED from the callable.
    template<template<typename> class Family, typename Handler>
    procedure<detail::handler_signature_t<Handler>, Family> serve(std::string_view fqn, Handler handler)
    {
        static_assert(detail::deducible_handler<Handler>,
                      "plexus: spell Sig explicitly — a generic lambda or overloaded call "
                      "operator has no single deducible signature; name the signature on the "
                      "endpoint");
        using Sig = detail::handler_signature_t<Handler>;
        return procedure<Sig, Family>{*this, fqn, std::move(handler)};
    }

    template<typename Sig, template<typename> class Family>
    plexus::caller<Sig, Family> caller(std::string_view fqn)
    {
        return plexus::caller<Sig, Family>{*this, fqn};
    }

    template<template<typename> class Family, typename Cb>
    subscriber<Family<detail::subscriber_value_t<Cb>>> subscribe(std::string_view topic, Cb cb)
    {
        static_assert(detail::deducible_handler<Cb>,
                      "plexus: spell the value type explicitly — a generic lambda or "
                      "overloaded call operator has no single deducible signature; name the "
                      "type on the endpoint");
        using T = detail::subscriber_value_t<Cb>;
        return subscriber<Family<T>>{*this, topic, std::move(cb)};
    }

    template<typename Codec>
    publisher<Codec> advertise(std::string_view topic, const typed_publisher_options &opts = {}, Codec codec = {})
    {
        return publisher<Codec>{*this, topic, opts, std::move(codec)};
    }

    template<typename Codec, typename Projection = no_projection>
    value_logger<Codec, Projection> log(std::string_view topic, const value_logger_options &opts, Codec codec = {}, Projection projection = {})
    {
        return value_logger<Codec, Projection>(*this, topic, opts, std::move(codec), std::move(projection));
    }

    // An RAII recorder handle that registers its tap on the engine and deregisters before
    // teardown. The sink MUST outlive the handle; the drain rides this node's executor turns.
    recorder<engine_type, Policy> make_recorder(io::recording::byte_sink &sink, recorder_options opts = {})
    {
        return recorder<engine_type, Policy>{m_engine, m_executor, m_id, sink, std::move(opts), m_wire_crypto_position};
    }

    friend struct detail::peer_watch<basic_node>;

private:
    using object_entry = detail::object_entry;
    using subscription = detail::subscription;
    using peer_watch   = detail::peer_watch<basic_node>;

    using registration_id = std::uint64_t;

    graph::snapshot_result topics_of(const plexus::node_id &node, graph::topic_role role, std::span<graph::topic_record> out) const
    {
        return detail::sweep_topics(m_engine.topic_table(), out,
                                    [&](const graph::topic_record &rec) { return rec.node == node && rec.role == role; });
    }

    registration_id register_subscriber_seam(std::string_view fqn, const io::subscriber_qos &qos,
                                             plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb,
                                             std::optional<std::uint64_t> type_id = std::nullopt, std::string_view type_name = {}, object_entry obj = {},
                                             std::optional<io::topic_capture_rule> capture = std::nullopt)
    {
        const registration_id rid    = m_next_registration++;
        const bool first_for_fqn     = !any_subscriber_for(fqn);
        std::pair<registration_id, subscription> entry{rid, subscription{std::string{fqn}, qos, type_id, std::string{type_name}, std::move(cb), std::move(obj)}};
        if(m_dispatch_depth > 0)
            m_deferred_subscription_adds.push_back(std::move(entry));
        else
            m_subscriptions.push_back(std::move(entry));
        if(first_for_fqn)
            self_attach(fqn, qos, type_id);
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
        provision_same_host_ring(fqn, subscriber_effective_bytes(qos), nullptr);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::subscriber_registered, wire::fqn_topic_hash(fqn), type_id});
        for(const auto &peer : m_known_peers)
            m_engine.subscribe(peer, fqn, qos, io::locality::any, io::reliability_requirement::any, type_id, type_name);
        return rid;
    }

    void retire_subscriber_seam(registration_id rid)
    {
        if(m_dispatch_depth > 0)
        {
            // A retire from inside a dispatch fan: record a logical removal and defer the physical
            // erase, so the in-flight callback loop never relocates m_subscriptions under itself.
            const std::optional<std::string> fqn = subscription_fqn(rid);
            if(!fqn || subscription_pending_removed(rid))
                return;
            m_deferred_subscription_removes.push_back(rid);
            if(!any_subscriber_for(*fqn))
                retire_fqn_demand(*fqn);
            return;
        }

        auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(), [&](const auto &e) { return e.first == rid; });
        if(it == m_subscriptions.end())
            return;
        const std::string fqn = it->second.fqn;
        m_subscriptions.erase(it);
        if(!any_subscriber_for(fqn))
            retire_fqn_demand(fqn);
    }

    void retire_fqn_demand(std::string_view fqn)
    {
        self_detach(fqn);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::subscriber_retired, wire::fqn_topic_hash(fqn), std::nullopt});
        for(const auto &peer : m_known_peers)
            m_engine.unsubscribe(peer, fqn);
    }

    // The declaration persists for the node's life (stable identity for subscriber correlation);
    // the handle's lifetime is distinct, so retire_publisher_seam only posts the drop edge.
    void declare_publisher_seam(std::string_view fqn, const topic_qos &qos, bool emit_source_identity, std::optional<std::uint64_t> type_id = std::nullopt,
                                std::string_view type_name = {}, std::uint64_t schema_hint = 0, const void *geometry = nullptr, std::optional<io::topic_capture_rule> capture = std::nullopt)
    {
        m_engine.messages().declare(fqn, qos, type_id, emit_source_identity, type_name, schema_hint);
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::publisher_declared, wire::fqn_topic_hash(fqn), type_id, type_name, schema_hint});
        const std::size_t effective_bytes = io::effective_max(qos, m_max_message_bytes);
        provision_same_host_ring(fqn, effective_bytes, geometry);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
        note_local_publisher(fqn);
    }

    void retire_publisher_seam(std::string_view fqn)
    {
        forget_local_publisher(fqn);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::publisher_dropped, wire::fqn_topic_hash(fqn), std::nullopt});
    }

    // An absent per-topic override (null) resolves to each shm member's own default_geometry(); a
    // composition with no shm member is a no-op. The override is the opaque carrier the declare
    // seam crosses; each member recovers its own geometry type from it member-side.
    void provision_same_host_ring(std::string_view fqn, std::size_t effective_bytes, const void *geometry)
    {
        std::string key{fqn};
        for_each_shm_member([&](auto &m) { m.set_topic_geometry(key, effective_bytes, m.geometry_from(geometry)); });
    }

    std::size_t subscriber_effective_bytes(const io::subscriber_qos &qos) const noexcept
    {
        return qos.requested_max_message_bytes != 0 ? static_cast<std::size_t>(qos.requested_max_message_bytes) : m_max_message_bytes;
    }

    // No shm member is inert: no gate and no policy installed.
    void install_upgrade_coordinator()
    {
        for_each_shm_member(
                [this](auto &m)
                {
                    m_engine.on_upgrade_policy(m.upgrade_policy());
                    detail::install_same_host_upgrade(m_engine, m);
                });
    }

    // The if-constexpr capability check makes a composition with no shm member a compile-time no-op.
    template<typename F>
    void for_each_shm_member(F &&fn)
    {
        if constexpr(sizeof...(Transports) == 1)
        {
            if constexpr(has_topic_geometry<engine_transport>)
                fn(m_leaf);
        }
        else
        {
            m_leaf.for_each_member(
                    [&](auto &m)
                    {
                        using M = std::remove_reference_t<decltype(m)>;
                        if constexpr(has_topic_geometry<M>)
                            fn(m);
                    });
        }
    }

    // Keys on the member's name-free geometry surface (default_geometry + the opaque-pointer
    // recover) rather than the concrete override type, so the probe names no shm type: a member
    // that has these provisions its own ring; a plain leaf is a compile-time no-op.
    template<typename M>
    static constexpr bool has_topic_geometry = requires(M &m) {
        m.set_topic_geometry(std::string{}, std::size_t{}, m.default_geometry());
        m.geometry_from(static_cast<const void *>(nullptr));
    };

    // Routes to the first connection-order peer with a complete session. With NO connected
    // provider the completion is POSTED with an absent status (the out-of-band no_provider
    // verdict, never a fabricated rpc_status) — it never hangs, buffers, or queues.
    template<typename OnReply>
    void call_seam(std::string_view fqn, std::span<const std::byte> param, OnReply on_reply, std::optional<std::chrono::nanoseconds> deadline)
    {
        for(const auto &peer : m_known_peers)
        {
            auto *session = m_engine.registry().session_for(peer);
            if(session == nullptr || !session->is_complete())
                continue;
            const std::optional<publisher_gid> provider{publisher_gid{peer, 0}};
            m_engine.procedures().call(
                    session->rpc_peer(), fqn, param, [on_reply = std::move(on_reply), provider](wire::rpc_status status, std::span<const std::byte> bytes) mutable
                    { on_reply(status, bytes, provider); }, deadline, session->session_id());
            return;
        }
        Policy::post(m_executor, [on_reply = std::move(on_reply)]() mutable { on_reply(std::nullopt, {}, std::nullopt); });
    }

    // The served-FQN set is checked BEFORE the forwarder is touched, so a refused (duplicate-local)
    // registration throws with ZERO side effects, closing the forwarder's silent-overwrite hijack.
    void serve_procedure_seam(std::string_view fqn, io::handler_fn handler)
    {
        if(std::find(m_served_fqns.begin(), m_served_fqns.end(), fqn) != m_served_fqns.end())
            plexus::detail::fail_closed("plexus: a procedure is already served locally on this fqn");
        m_served_fqns.emplace_back(fqn);
        m_engine.procedures().serve(fqn, std::move(handler));
    }

    void retire_procedure_seam(std::string_view fqn)
    {
        m_engine.procedures().retire(fqn);
        std::erase(m_served_fqns, std::string{fqn});
    }

    // Insertion order is awareness order, which also feeds caller target resolution.
    void note_known_peer(const plexus::node_id &id)
    {
        if(std::find(m_known_peers.begin(), m_known_peers.end(), id) == m_known_peers.end())
            m_known_peers.push_back(id);
    }

    // Idempotent per (peer, fqn).
    void fan_demands_to(const plexus::node_id &id)
    {
        for(const auto &[rid, sub] : m_subscriptions)
            m_engine.subscribe(id, sub.fqn, sub.qos, io::locality::any, io::reliability_requirement::any, sub.type_id, sub.type_name);
    }

    // While a dispatch fan is live (m_dispatch_depth > 0) the (un)subscribe seams cannot touch
    // m_subscriptions directly: a push_back could reallocate it while the callback loop iterates it,
    // and an erase would relocate the very callable being invoked. Edits are staged and applied by the
    // outermost fan on exit; a logically-removed entry is skipped for the remainder of the fan.
    struct dispatch_guard
    {
        basic_node &owner;
        explicit dispatch_guard(basic_node &node) noexcept
                : owner(node)
        {
            ++owner.m_dispatch_depth;
        }
        dispatch_guard(const dispatch_guard &)            = delete;
        dispatch_guard &operator=(const dispatch_guard &) = delete;
        ~dispatch_guard()
        {
            if(--owner.m_dispatch_depth != 0)
                return;
#if defined(__cpp_exceptions)
            try
            {
                owner.apply_deferred_subscription_edits();
            }
            catch(...)
            {
            }
#else
            owner.apply_deferred_subscription_edits();
#endif
        }
    };

    bool subscription_pending_removed(registration_id rid) const
    {
        return std::find(m_deferred_subscription_removes.begin(), m_deferred_subscription_removes.end(), rid) != m_deferred_subscription_removes.end();
    }

    std::optional<std::string> subscription_fqn(registration_id rid) const
    {
        for(const auto &e : m_subscriptions)
            if(e.first == rid)
                return e.second.fqn;
        for(const auto &e : m_deferred_subscription_adds)
            if(e.first == rid)
                return e.second.fqn;
        return std::nullopt;
    }

    void apply_deferred_subscription_edits()
    {
        if(!m_deferred_subscription_removes.empty())
        {
            std::erase_if(m_subscriptions, [&](const auto &e) { return subscription_pending_removed(e.first); });
            std::erase_if(m_deferred_subscription_adds, [&](const auto &e) { return subscription_pending_removed(e.first); });
            m_deferred_subscription_removes.clear();
        }
        for(auto &e : m_deferred_subscription_adds)
            m_subscriptions.push_back(std::move(e));
        m_deferred_subscription_adds.clear();
    }

    void dispatch_message(std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info)
    {
        dispatch_guard guard{*this};
        for(auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn && !subscription_pending_removed(rid))
                sub.cb(bytes, info);
    }

    // A native_key MATCH dispatches the concrete T for the callback's duration only (the session
    // releases the slot right after); a MISMATCH is counted and warn-and-dropped, never a cast.
    void dispatch_object(std::string_view fqn, const io::object_carrier &carrier)
    {
        dispatch_guard guard{*this};

        // The receive clock is read at most once, and only if a matching subscriber wants
        // message_info.
        std::uint64_t reception = 0;
        bool reception_read     = false;

        for(auto &[rid, sub] : m_subscriptions)
        {
            if(sub.fqn != fqn || sub.obj.native_key == nullptr || subscription_pending_removed(rid))
                continue;
            if(sub.obj.native_key != carrier.native_key)
            {
                ++m_object_dispatch_mismatch;
                m_logger.warn("plexus: node object_native_key_mismatch");
                continue;
            }
            sub.obj.dispatch(carrier, object_info_for(sub, carrier, reception, reception_read));
        }
    }

    // reception / reception_read are threaded by reference so the receive clock is read once on
    // first need and shared across the fan; a subscriber wanting no info gets a 0 stamp.
    io::message_info object_info_for(const subscription &sub, const io::object_carrier &carrier, std::uint64_t &reception, bool &reception_read) const
    {
        io::message_info info{};
        info.publication_sequence = carrier.sequence;
        info.from_intra_process   = true;
        if(sub.qos.wants_message_info)
        {
            if(!reception_read)
            {
                reception      = wire::now_timestamp_ns();
                reception_read = true;
            }
            info.source_timestamp    = carrier.source_timestamp;
            info.reception_timestamp = reception;
        }
        return info;
    }

    bool any_subscriber_for(std::string_view fqn) const
    {
        const auto live = [&](const auto &e) { return e.second.fqn == fqn && !subscription_pending_removed(e.first); };
        return std::any_of(m_subscriptions.begin(), m_subscriptions.end(), live) || std::any_of(m_deferred_subscription_adds.begin(), m_deferred_subscription_adds.end(), live);
    }

    // The registry-facing self-route: the concrete loopback channel on a single-transport node, or
    // the reference-adapter wrapper (the registry's polymorphic_byte_channel) on a multi-transport one.
    engine_channel &self_route() noexcept
    {
        if constexpr(k_self_route_erased)
            return m_self_route;
        else
            return m_self_channel;
    }

    void install_self_loopback()
    {
        if constexpr(k_has_self_loopback)
        {
            m_self_channel.on_object([this](const io::object_carrier &carrier) { loopback_object(carrier); });
            m_self_channel.on_data([this](std::span<const std::byte> framed) { loopback_bytes(framed); });
        }
    }

    // The object self-route: resolve the fqn from the carrier's topic_hash and re-enter the node's
    // own dispatch (which re-scans m_subscriptions, so a retired sub is a safe no-op). The carrier
    // arrives addref'd by the channel's posted send_object; the channel releases after this returns.
    void loopback_object(const io::object_carrier &carrier)
    {
        const std::string_view fqn = m_engine.messages().fqn_for(carrier.topic_hash);
        if(!fqn.empty())
            dispatch_object(fqn, carrier);
    }

    // The bytes self-route: strip the frame header off the posted frame and route the inner through
    // the forwarder's decode (fqn resolve + intra-process stamp) into the node's own dispatch.
    void loopback_bytes(std::span<const std::byte> framed)
    {
        if constexpr(k_has_self_loopback)
        {
            const auto hdr = wire::decode_header(framed);
            if(!hdr)
                return;
            const std::span<const std::byte> inner       = framed.subspan(wire::header_size);
            const bool has_source_identity               = (hdr->flags & wire::k_flag_source_identity) != 0;
            io::message_info info{};
            info.from_intra_process = true;
            typename io::message_forwarder<engine_policy>::peer self{self_route(), m_service_name};
            m_engine.messages().deliver(self, inner, info, m_id, has_source_identity,
                                        [this](std::string_view fqn, std::span<const std::byte> data, const io::message_info &mi) { dispatch_message(fqn, data, mi); });
        }
    }

    // Compile-time route selection: the intra-node self-channel when the pack carries one, else the
    // shm self-ring fallback when an shm member is present. The two are mutually exclusive per node.
    // The intra-node attach is a free registry entry, so it engages on every first-subscribe; the
    // shm self-ring mints an OS region, so it engages only when a same-node pub+sub actually coexist.
    void self_attach(std::string_view fqn, const io::subscriber_qos &qos, std::optional<std::uint64_t> type_id)
    {
        if constexpr(k_has_self_loopback)
            m_engine.messages().attach_local(fqn, self_route(), m_service_name, qos, type_id);
        else if constexpr(k_has_shm_self_carrier)
            sync_shm_self_carrier(fqn);
    }

    void self_detach(std::string_view fqn)
    {
        if constexpr(k_has_self_loopback)
            m_engine.messages().detach_local(fqn, self_route());
        else if constexpr(k_has_shm_self_carrier)
            sync_shm_self_carrier(fqn);
    }

    void note_local_publisher(std::string_view fqn)
    {
        if constexpr(k_has_shm_self_carrier)
        {
            ++m_local_publishers[std::string{fqn}];
            sync_shm_self_carrier(fqn);
        }
    }

    void forget_local_publisher(std::string_view fqn)
    {
        if constexpr(k_has_shm_self_carrier)
        {
            auto it = m_local_publishers.find(std::string{fqn});
            if(it != m_local_publishers.end() && --it->second == 0)
                m_local_publishers.erase(it);
            sync_shm_self_carrier(fqn);
        }
    }

    // The shm self-ring mints/holds an OS region, so it engages ONLY when a same-node publisher AND
    // subscriber coexist for the fqn (true self-traffic) — a node talking to a distinct-host peer
    // creates no region. Idempotent: re-driven from all four pub/sub seams to converge on the state.
    void sync_shm_self_carrier(std::string_view fqn)
    {
        const std::string key{fqn};
        const bool want = m_local_publishers.contains(key) && any_subscriber_for(fqn);
        const bool have = m_self_carriers.contains(key);
        if(want && !have)
            install_shm_self_carrier(key);
        else if(!want && have)
            teardown_shm_self_carrier(key);
    }

    // Mint the node-local shm self-ring for the fqn, record its write half as the self-route (a
    // publish fans into the ring), and bind the read half to re-enter the node's own dispatch.
    void install_shm_self_carrier(const std::string &fqn)
    {
        const io::subscriber_qos qos          = subscriber_qos_for(fqn);
        const std::optional<std::uint64_t> tid = subscriber_type_id_for(fqn);
        for_each_shm_member(
                [&](auto &m)
                {
                    auto carrier = detail::install_self_carrier<engine_channel>(m, fqn, [this](std::span<const std::byte> framed) { self_carrier_bytes(framed); });
                    if(!carrier.write)
                        return;
                    m_engine.messages().attach_local(fqn, *carrier.write, m_service_name, qos, tid);
                    m_self_carriers.insert_or_assign(fqn, std::move(carrier));
                });
    }

    void teardown_shm_self_carrier(const std::string &fqn)
    {
        auto it = m_self_carriers.find(fqn);
        if(it == m_self_carriers.end())
            return;
        m_engine.messages().detach_local(fqn, *it->second.write);
        m_self_carriers.erase(it);
    }

    io::subscriber_qos subscriber_qos_for(std::string_view fqn) const
    {
        for(const auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                return sub.qos;
        return {};
    }

    std::optional<std::uint64_t> subscriber_type_id_for(std::string_view fqn) const
    {
        for(const auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                return sub.type_id;
        return std::nullopt;
    }

    // The shm self-ring's read half: strip the frame header off the ring's framed bytes and route the
    // inner through the forwarder's decode into the node's own dispatch — bypassing the session-keyed
    // inject_upgrade_receive (a node has no session to itself). The frame's write-half channel is the
    // (ignored) peer the deliver overload requires.
    void self_carrier_bytes(std::span<const std::byte> framed)
    {
        if constexpr(k_has_shm_self_carrier)
        {
            const auto hdr = wire::decode_header(framed);
            if(!hdr)
                return;
            const std::span<const std::byte> inner = framed.subspan(wire::header_size);
            const bool has_source_identity         = (hdr->flags & wire::k_flag_source_identity) != 0;
            io::message_info info{};
            info.from_intra_process = true;
            typename io::message_forwarder<engine_policy>::peer self{any_self_carrier_channel(), m_service_name};
            m_engine.messages().deliver(self, inner, info, m_id, has_source_identity,
                                        [this](std::string_view fqn, std::span<const std::byte> data, const io::message_info &mi) { dispatch_message(fqn, data, mi); });
        }
    }

    // Any live carrier's write half supplies the channel reference the deliver peer needs (the peer is
    // never dereferenced); the receive sink only fires while a carrier is live, so the map is non-empty.
    engine_channel &any_self_carrier_channel() noexcept
    {
        return *m_self_carriers.begin()->second.write;
    }

    // Each verb is a captureless static lambda (a plain fn-ptr, zero alloc) recovering the node and
    // forwarding to the private *_seam, so the concrete Policy stays inside those bodies.
    // NOLINTNEXTLINE(readability-function-size)
    io::endpoint_seam endpoint_seam_for() noexcept
    {
        io::endpoint_seam s{};
        s.ctx               = this;
        s.declare_publisher = [](void *ctx, std::string_view fqn, const topic_qos &qos, bool emit, std::optional<std::uint64_t> type_id, std::string_view type_name,
                                 std::uint64_t schema_hint, const void *geometry, std::optional<io::topic_capture_rule> capture)
        { static_cast<basic_node *>(ctx)->declare_publisher_seam(fqn, qos, emit, type_id, type_name, schema_hint, geometry, capture); };
        s.publish        = [](void *ctx, std::string_view fqn, std::span<const std::byte> bytes) { static_cast<basic_node *>(ctx)->m_engine.messages().publish(fqn, bytes); };
        s.publish_object = [](void *ctx, std::string_view fqn, const io::object_carrier &carrier, io::encode_thunk encode)
        { static_cast<basic_node *>(ctx)->m_engine.messages().publish_object(fqn, carrier, [&] { return io::invoke(encode); }); };
        s.register_subscriber = [](void *ctx, std::string_view fqn, const io::subscriber_qos &qos, io::bytes_cb cb, std::optional<std::uint64_t> type_id, std::string_view type_name,
                                   const void *native_key, io::object_dispatch dispatch, std::optional<io::topic_capture_rule> capture) -> registration_id
        { return static_cast<basic_node *>(ctx)->register_subscriber_seam(fqn, qos, std::move(cb), type_id, type_name, object_entry{native_key, std::move(dispatch)}, capture); };
        s.retire_subscriber = [](void *ctx, registration_id rid) { static_cast<basic_node *>(ctx)->retire_subscriber_seam(rid); };
        s.retire_publisher  = [](void *ctx, std::string_view fqn) { static_cast<basic_node *>(ctx)->retire_publisher_seam(fqn); };
        s.serve_procedure   = [](void *ctx, std::string_view fqn, io::handler_fn handler) { static_cast<basic_node *>(ctx)->serve_procedure_seam(fqn, std::move(handler)); };
        s.retire_procedure  = [](void *ctx, std::string_view fqn) { static_cast<basic_node *>(ctx)->retire_procedure_seam(fqn); };
        s.call              = [](void *ctx, std::string_view fqn, std::span<const std::byte> param, io::on_reply_fn on_reply, std::optional<std::chrono::nanoseconds> deadline)
        { static_cast<basic_node *>(ctx)->call_seam(fqn, param, std::move(on_reply), deadline); };
        return s;
    }

    static io::handshake_fsm_config make_fsm_cfg(const plexus::node_id &id, const node_options &opts)
    {
        return io::handshake_fsm_config{.self_id                  = id,
                                        .version_major            = opts.handshake.version_major,
                                        .version_minor            = opts.handshake.version_minor,
                                        .compatible_version_major = opts.handshake.compatible_version_major,
                                        .compatible_version_minor = opts.handshake.compatible_version_minor,
                                        .local_fingerprint        = opts.handshake.local_fingerprint,
                                        .attach_policy            = opts.attach_policy,
                                        .handshake_retry          = opts.handshake_retry};
    }

    log::logger &resolve_logger(const node_options &opts)
    {
        return opts.logger != nullptr ? *opts.logger : m_default_logger;
    }

    // The one borrowed transport, or the node-owned mux glue when composing several.
    engine_transport &engine_leaf([[maybe_unused]] Transports &...transports) noexcept
    {
        if constexpr(sizeof...(Transports) == 1)
            return std::get<0>(std::tie(transports...));
        else
            return m_glue;
    }

    // The host is empty until the first listen. A real mDNS backend fills it from the resolved
    // record; over static_discovery the node supplies it.
    void advertise_card()
    {
        m_disc.advertise({m_service_name, io::endpoint{"", m_host}, discovery::assemble_contact_card(m_id, m_listens)});
    }

    // The card is untrusted multicast input: a malformed/missing node_id, a self card, a host-less
    // address, or no usable port key each abort without a note_peer. The first valid port key wins.
    void note_from_card(const discovery::service_info &peer)
    {
        const auto peer_id = plexus::detail::card_node_id(peer.metadata);
        if(!peer_id || *peer_id == m_id)
            return;
        const std::string host = plexus::detail::host_of(peer.endpoint.address);
        if(host.empty())
            return;
        if(const auto ep = plexus::detail::first_port_endpoint(peer.metadata, host))
        {
            m_engine.note_peer(*peer_id, *ep);
            note_known_peer(*peer_id);
            fan_demands_to(*peer_id);
        }
    }

    // A goodbye removes awareness only (engine forget + local mirror erase), mirroring
    // note_from_card's id/self guards; an unknown or self id is a harmless no-op.
    void forget_from_card(const discovery::service_info &peer)
    {
        const auto peer_id = plexus::detail::card_node_id(peer.metadata);
        if(!peer_id || *peer_id == m_id)
            return;
        m_engine.forget(*peer_id);
        std::erase(m_known_peers, *peer_id);
    }

    plexus::node_id m_id;
    executor_type m_executor;
    discovery::discovery &m_disc;
    // Declared immediately ABOVE m_logger so it is fully constructed before resolve_logger binds
    // either reference in the init list when no logger is supplied.
    log::null_logger m_default_logger;
    log::logger &m_logger;
    std::string m_service_name;
    std::string m_host;
    std::vector<discovery::listening_transport> m_listens;
    std::size_t m_max_message_bytes;
    wire_crypto_position m_wire_crypto_position;

    // The non-movable mux is never a prvalue, so the empty single-transport glue keeps its
    // zero-size overlap under [[no_unique_address]].
    [[no_unique_address]] detail::node_mux_glue<Transports...> m_glue;

    engine_transport &m_leaf;

    // Declared BEFORE the engine (which captures &m_peer_watch and a `this`-bound route) so the
    // engine is destroyed FIRST, leaving no dangling observer/route reference during teardown.
    peer_watch m_peer_watch{*this};
    std::vector<plexus::node_id> m_known_peers;
    std::vector<std::pair<registration_id, subscription>> m_subscriptions;

    // Dispatch-fan re-entrancy guard: a (un)subscribe from inside a callback stages its edit here
    // rather than mutating m_subscriptions under the live callback loop (see dispatch_guard).
    int m_dispatch_depth{0};
    std::vector<std::pair<registration_id, subscription>> m_deferred_subscription_adds;
    std::vector<registration_id> m_deferred_subscription_removes;

    std::vector<std::string> m_served_fqns;
    registration_id m_next_registration{1};
    std::size_t m_object_dispatch_mismatch{};

    // The node-owned self-route, attached to its OWN forwarder so a publish reaches a same-node
    // subscriber. Declared BEFORE m_engine so it outlives the registry references that hold it.
    // It is only wired when the pack carries an intra-node transport; on any other node it is idle.
    io::process_loopback_channel<Policy> m_self_channel;

    // On a multi-transport node the registry holds reference_wrapper<polymorphic_byte_channel>, so the
    // concrete self-channel is erased through a non-owning reference adapter (the node keeps owning
    // m_self_channel). [[no_unique_address]] on the single-transport monostate keeps it zero-size.
    using self_route_holder = std::conditional_t<k_self_route_erased, io::polymorphic_byte_channel, detail::no_self_route_wrapper>;
    [[no_unique_address]] self_route_holder m_self_route{detail::make_self_route<self_route_holder>(m_self_channel)};

    // Live local-publisher count per fqn: the shm self-ring mints only when a same-node publisher and
    // subscriber coexist, so the carrier's OS region is never created for a node that only talks remote.
    std::unordered_map<std::string, std::size_t> m_local_publishers;

    // The per-fqn node-local shm self-ring carriers (the fallback self-route when no intra-node
    // transport serves self). Each holds the write half recorded in the registry + the read half's
    // RAII handle. Declared BEFORE m_engine so the registry's references outlive the engine teardown.
    std::unordered_map<std::string, detail::self_carrier_handle<engine_channel>> m_self_carriers;

    engine_type m_engine;
};

// Transitional: every existing plexus::node<...> spelling migrates onto basic_node through this
// alias. It retires once call sites respell, freeing the plexus::node name for the eventual
// non-templated native default.
template<typename Policy, typename... Transports>
using node = basic_node<Policy, Transports...>;

}

#endif
