#ifndef HPP_GUARD_PLEXUS_NODE_H
#define HPP_GUARD_PLEXUS_NODE_H

#include "plexus/recorder.h"
#include "plexus/node_options.h"

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
#include "plexus/io/multiplexing_transport.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/log/logger.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/muxify.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/policy.h"
#include "plexus/transport_priority.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/fail_closed.h"
#include "plexus/detail/address_parse.h"
#include "plexus/detail/node_upgrade_wiring.h"
#include "plexus/detail/function_traits.h"
#include "plexus/detail/node_internals.h"

#include <span>
#include <tuple>
#include <string>
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

struct typed_publisher_options;
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
class node
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
    using executor_type    = typename Policy::executor_type;
    using engine_policy    = detail::node_engine_policy<Policy, Transports...>;
    using engine_transport = detail::node_engine_transport<Transports...>;
    using engine_type      = io::routing_engine<engine_policy, engine_transport>;
    using engine_channel   = typename engine_type::channel_type;
    using transport_tuple  = std::tuple<Transports...>;

    // The node_id is taken verbatim: plexus compares the identity, never mints or interprets it.
    node(executor_type executor, discovery::discovery &disc, const plexus::node_id &id, Transports &...transports, const node_options &opts)
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
            , m_engine(m_leaf, executor, make_fsm_cfg(id, opts), opts.handshake_timeout, opts.reconnect, opts.redial_seed, resolve_logger(opts), opts.dial_eagerly,
                       opts.max_message_bytes)
    {
        // The routes are installed and the card advertised synchronously in the ctor turn, before
        // any session is built (the set-before-listen contract).
        m_engine.on_message_route([this](std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info) { dispatch_message(fqn, bytes, info); });
        m_engine.on_object_route([this](std::string_view fqn, const io::object_carrier &carrier) { dispatch_object(fqn, carrier); });
        m_engine.add_observer(m_peer_watch);
        advertise_card();
        m_disc.browse([this](const discovery::service_info &peer) { note_from_card(peer); });
        install_upgrade_coordinator();
        m_engine.capture().set_default(opts.capture.to_rule());
        m_engine.post_participant({io::participant_edge::created, m_id});
    }

    node(executor_type executor, discovery::discovery &disc, std::string_view name, Transports &...transports, const node_options &opts)
            : node(executor, disc, detail::hash_node_id(name), transports..., opts)
    {
    }

    node(const node &)            = delete;
    node &operator=(const node &) = delete;
    node(node &&)                 = delete;
    node &operator=(node &&)      = delete;

    // The peer watcher is deregistered FIRST so the teardown edge's observer snapshot (captured in
    // place of the engine, which is destroyed before the executor drains the closure) holds only
    // externally-owned observers that outlive the drain. A dtor must not throw — the post is wrapped.
    ~node()
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

    friend struct detail::peer_watch<node>;

private:
    using object_entry = detail::object_entry;
    using subscription = detail::subscription;
    using peer_watch   = detail::peer_watch<node>;

    using registration_id = std::uint64_t;

    registration_id register_subscriber_seam(std::string_view fqn, const io::subscriber_qos &qos,
                                             plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb,
                                             std::optional<std::uint64_t> type_id = std::nullopt, object_entry obj = {}, std::optional<io::topic_capture_rule> capture = std::nullopt)
    {
        const registration_id rid = m_next_registration++;
        m_subscriptions.push_back({rid, subscription{std::string{fqn}, qos, type_id, std::move(cb), std::move(obj)}});
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
        provision_same_host_ring(fqn, subscriber_effective_bytes(qos), nullptr);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::subscriber_registered, wire::fqn_topic_hash(fqn), type_id});
        for(const auto &peer : m_known_peers)
            m_engine.subscribe(peer, fqn, qos, io::locality::any, io::reliability_requirement::any, type_id);
        return rid;
    }

    void retire_subscriber_seam(registration_id rid)
    {
        auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(), [&](const auto &e) { return e.first == rid; });
        if(it == m_subscriptions.end())
            return;
        const std::string fqn = it->second.fqn;
        m_subscriptions.erase(it);
        if(!any_subscriber_for(fqn))
        {
            m_engine.post_endpoint(fqn, {io::endpoint_edge::subscriber_retired, wire::fqn_topic_hash(fqn), std::nullopt});
            for(const auto &peer : m_known_peers)
                m_engine.unsubscribe(peer, fqn);
        }
    }

    // The declaration persists for the node's life (stable identity for subscriber correlation);
    // the handle's lifetime is distinct, so retire_publisher_seam only posts the drop edge.
    void declare_publisher_seam(std::string_view fqn, const topic_qos &qos, bool emit_source_identity, std::optional<std::uint64_t> type_id = std::nullopt,
                                const void *geometry = nullptr, std::optional<io::topic_capture_rule> capture = std::nullopt)
    {
        m_engine.messages().declare(fqn, qos, type_id, emit_source_identity);
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::publisher_declared, wire::fqn_topic_hash(fqn), type_id});
        const std::size_t effective_bytes = io::effective_max(qos, m_max_message_bytes);
        provision_same_host_ring(fqn, effective_bytes, geometry);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
    }

    void retire_publisher_seam(std::string_view fqn)
    {
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
            m_engine.subscribe(id, sub.fqn, sub.qos, io::locality::any, io::reliability_requirement::any, sub.type_id);
    }

    void dispatch_message(std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info)
    {
        for(auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                sub.cb(bytes, info);
    }

    // A native_key MATCH dispatches the concrete T for the callback's duration only (the session
    // releases the slot right after); a MISMATCH is counted and warn-and-dropped, never a cast.
    void dispatch_object(std::string_view fqn, const io::object_carrier &carrier)
    {
        // The receive clock is read at most once, and only if a matching subscriber wants
        // message_info.
        std::uint64_t reception = 0;
        bool reception_read     = false;

        for(auto &[rid, sub] : m_subscriptions)
        {
            if(sub.fqn != fqn || sub.obj.native_key == nullptr)
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
        return std::any_of(m_subscriptions.begin(), m_subscriptions.end(), [&](const auto &e) { return e.second.fqn == fqn; });
    }

    // Each verb is a captureless static lambda (a plain fn-ptr, zero alloc) recovering the node and
    // forwarding to the private *_seam, so the concrete Policy stays inside those bodies.
    // NOLINTNEXTLINE(readability-function-size)
    io::endpoint_seam endpoint_seam_for() noexcept
    {
        io::endpoint_seam s{};
        s.ctx               = this;
        s.declare_publisher = [](void *ctx, std::string_view fqn, const topic_qos &qos, bool emit, std::optional<std::uint64_t> type_id, const void *geometry,
                                 std::optional<io::topic_capture_rule> capture)
        { static_cast<node *>(ctx)->declare_publisher_seam(fqn, qos, emit, type_id, geometry, capture); };
        s.publish        = [](void *ctx, std::string_view fqn, std::span<const std::byte> bytes) { static_cast<node *>(ctx)->m_engine.messages().publish(fqn, bytes); };
        s.publish_object = [](void *ctx, std::string_view fqn, const io::object_carrier &carrier, io::encode_thunk encode)
        { static_cast<node *>(ctx)->m_engine.messages().publish_object(fqn, carrier, [&] { return io::invoke(encode); }); };
        s.register_subscriber = [](void *ctx, std::string_view fqn, const io::subscriber_qos &qos, io::bytes_cb cb, std::optional<std::uint64_t> type_id, const void *native_key,
                                   io::object_dispatch dispatch, std::optional<io::topic_capture_rule> capture) -> registration_id
        { return static_cast<node *>(ctx)->register_subscriber_seam(fqn, qos, std::move(cb), type_id, object_entry{native_key, std::move(dispatch)}, capture); };
        s.retire_subscriber = [](void *ctx, registration_id rid) { static_cast<node *>(ctx)->retire_subscriber_seam(rid); };
        s.retire_publisher  = [](void *ctx, std::string_view fqn) { static_cast<node *>(ctx)->retire_publisher_seam(fqn); };
        s.serve_procedure   = [](void *ctx, std::string_view fqn, io::handler_fn handler) { static_cast<node *>(ctx)->serve_procedure_seam(fqn, std::move(handler)); };
        s.retire_procedure  = [](void *ctx, std::string_view fqn) { static_cast<node *>(ctx)->retire_procedure_seam(fqn); };
        s.call              = [](void *ctx, std::string_view fqn, std::span<const std::byte> param, io::on_reply_fn on_reply, std::optional<std::chrono::nanoseconds> deadline)
        { static_cast<node *>(ctx)->call_seam(fqn, param, std::move(on_reply), deadline); };
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
                                        .attach_policy            = opts.attach_policy};
    }

    log::logger &resolve_logger(const node_options &opts)
    {
        return opts.logger != nullptr ? *opts.logger : m_default_logger;
    }

    // The one borrowed transport, or the node-owned mux glue when composing several.
    engine_transport &engine_leaf(Transports &...transports) noexcept
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
    std::vector<std::string> m_served_fqns;
    registration_id m_next_registration{1};
    std::size_t m_object_dispatch_mismatch{};

    engine_type m_engine;
};

}

#endif
