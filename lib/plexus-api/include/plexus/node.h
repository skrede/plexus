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
#include "plexus/detail/node_shm_wiring.h"
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

// One transport composes directly; two or more compose through a node-owned
// multiplexing_transport over muxify<Policy>. The selection is LAZY (a `::type` indirection,
// not std::conditional_t on the types) so the multiplexing_transport branch is NEVER
// instantiated for a single transport — eager instantiation would fail mux_member for a leaf
// whose concrete-channel completion shape does not satisfy the multiplexer's erased one.
template<typename Policy, typename... Transports>
using node_engine_policy = std::conditional_t<sizeof...(Transports) == 1, Policy, muxify<Policy>>;

// The empty single-transport glue under [[no_unique_address]] is zero-overhead. The variadic
// ctor lets the node's member-init use ONE in-place construction expression for both glue kinds
// (this type ignores the leaves; the mux consumes them), so the non-movable mux is never a
// prvalue copy-elision would have to thread into the member.
struct no_mux_glue
{
    no_mux_glue() = default;
    template<typename... Ignored>
    explicit no_mux_glue(Ignored &&...) noexcept
    {
    }
};

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
using node_engine_transport =
        typename mux_selection<sizeof...(Transports) == 1, Transports...>::transport;

template<typename... Transports>
using node_mux_glue = typename mux_selection<sizeof...(Transports) == 1, Transports...>::glue;

// FNV-1a 64 over the name, run twice with distinct offset baselines, folding the two 8-byte
// digests into the 16-byte node_id. Deterministic and platform-stable: the SAME name yields the
// SAME identity (an intentional property of the OPT-IN name overload, not a collision trap).
inline node_id hash_node_id(std::string_view name) noexcept
{
    constexpr std::uint64_t k_prime    = 1099511628211ull;
    constexpr std::uint64_t k_basis_lo = 1469598103934665603ull;
    constexpr std::uint64_t k_basis_hi = 0x9e3779b97f4a7c15ull;
    std::uint64_t           lo         = k_basis_lo;
    std::uint64_t           hi         = k_basis_hi;
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

// Derive a node_id from a name deterministically (OPT-IN; the verbatim node_id is the primary
// path) — equal to the id() of a node constructed with the same name string.
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

template<typename Sig = void, template<typename> class CReq = no_codec,
         template<typename> class CRes = CReq>
class caller;

template<typename Sig = void, template<typename> class CReq = no_codec,
         template<typename> class CRes = CReq>
class procedure;

// The consumable public surface: a node composes a routing_engine over an injected substrate.
// Policy is explicit (the plexus convention); the transport pack is deduced. Omitting any
// substrate element is a COMPILE ERROR — there is no owning convenience overload.
//
// LIFETIME: the node BORROWS its executor, discovery, and transport leaves and OWNS only the
// engine and (for >1 transport) the multiplexing glue. Every engine callback is POSTED on the
// borrowed executor and captures `this`. The borrowed substrate MUST outlive the node, and the
// executor MUST be drained before the node is destroyed — the structural single-owner
// discipline. The node pins `this` into those callbacks, so it is non-copyable and non-movable.
//
// over-limit: one cohesive public facade; the typed member factories and the private endpoint
// seams share the engine + subscription/peer/served-fqn state, so splitting the surface scatters
// that shared state (the shm-wiring glue already extracted to detail/node_shm_wiring.h).
template<typename Policy, typename... Transports>
    requires plexus::Policy<Policy> && (sizeof...(Transports) >= 1) &&
        (sizeof...(Transports) == 1
                 ? io::transport_backend<std::tuple_element_t<0, std::tuple<Transports..., void>>,
                                         Policy>
                 : (io::mux_member<Transports> && ...))
class node
{
    // The endpoint handles are the ONLY construction path: the registration/retire seams are
    // PRIVATE, reachable solely through these friends (no public node.publish/subscribe factory).
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

    // The node_id is taken VERBATIM: plexus compares the identity, never mints or interprets it.
    node(executor_type executor, discovery::discovery &disc, const plexus::node_id &id,
         Transports &...transports, const node_options &opts)
            : m_id(id)
            , m_executor(executor)
            , m_disc(disc)
            , m_logger(resolve_logger(opts))
            , m_service_name(opts.name.empty() ? io::node_name_of(id) : opts.name)
            , m_max_message_bytes(opts.max_message_bytes)
            , m_wire_crypto_position(opts.wire.position)
            // The leaves are BORROWED three ways and never consumed (mux glue, resolve_hook's
            // by-ref capture, engine_leaf's re-read), so a future edit must NOT move into the mux
            // ctor — that would invalidate the subsequent engine_leaf/resolve_hook reads.
            , m_glue(transports..., io::transport_selector{}, detail::resolve_hook(transports...))
            , m_leaf(engine_leaf(transports...))
            , m_engine(m_leaf, executor, make_fsm_cfg(id, opts), opts.handshake_timeout,
                       opts.reconnect, opts.redial_seed, resolve_logger(opts), opts.dial_eagerly,
                       opts.max_message_bytes)
    {
        // Install the node-shared routes ONCE, before any session is built (the
        // set-before-listen contract). Each fans a delivered frame to every callback locally
        // registered for its fqn; the object lane is native-key-checked (never a cast).
        m_engine.on_message_route([this](std::string_view fqn, std::span<const std::byte> bytes,
                                         const io::message_info &info)
                                  { dispatch_message(fqn, bytes, info); });
        m_engine.on_object_route([this](std::string_view fqn, const io::object_carrier &carrier)
                                 { dispatch_object(fqn, carrier); });
        // Re-fan a standing demand to a peer that becomes ready AFTER it was registered.
        m_engine.add_observer(m_peer_watch);
        // Advertise the (port-less) card so a dial-only node is discoverable from birth, and
        // browse to awareness. Synchronous in the ctor turn (the set-before-listen contract).
        advertise_card();
        m_disc.browse([this](const discovery::service_info &peer) { note_from_card(peer); });
        // Source the upgrade policy from the shm member (which owns its default + ceiling) and
        // install the shm mint gate when one exists; a composition with no shm member installs no
        // gate (inert). The per-ring slab ceiling already defaults on the member's registry.
        install_upgrade_coordinator();
        // The off default selects nothing, so a node that declares no recording QoS stays inert.
        m_engine.capture().set_default(opts.capture.to_rule());
        m_engine.post_participant({io::participant_edge::created, m_id});
    }

    // The name-hash overload (OPT-IN): derives the node_id from the name; the verbatim ctor is
    // the primary path.
    node(executor_type executor, discovery::discovery &disc, std::string_view name,
         Transports &...transports, const node_options &opts)
            : node(executor, disc, detail::hash_node_id(name), transports..., opts)
    {
    }

    node(const node &)            = delete;
    node &operator=(const node &) = delete;
    node(node &&)                 = delete;
    node &operator=(node &&)      = delete;

    // The destroy edge is POSTED before member teardown, so it surfaces only if the owner pumps
    // the executor after the node returns. The teardown variant captures an observer snapshot,
    // not the engine (the engine is destroyed before the executor drains this closure); the peer
    // watcher is deregistered FIRST so the snapshot holds only externally-owned observers that
    // outlive the drain. A dtor must not throw — the post is wrapped.
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

    // Bind, then append this transport's {scheme, port} to the live card and re-advertise.
    // PRECONDITION: an advertised listen requires an EXPLICIT port ("host:port") — a port-0
    // auto-assign cannot be advertised (the transport_backend concept exposes no bound-port
    // accessor), so it binds the engine but advertises no port key.
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

    const plexus::node_id &id() const noexcept { return m_id; }

    // Escape hatches: the live engine objects, for advanced peer-level work the topic-level
    // public verbs deliberately hide.
    engine_type       &router() noexcept { return m_engine; }
    const engine_type &router() const noexcept { return m_engine; }
    auto              &message_forwarder() noexcept { return m_engine.messages(); }
    executor_type      executor() const noexcept { return m_executor; }

    // Object-lane deliveries dropped at the demux on a type-witness mismatch — the never-UB
    // backstop's readable signal (never a cast, never silent).
    [[nodiscard]] std::size_t object_dispatch_mismatch() const noexcept
    {
        return m_object_dispatch_mismatch;
    }

    // ---- Member factories (partial-explicit deduction) ------------------------------
    //
    // The codec FAMILY is spelled explicitly (Family is NON-defaulted, sidestepping the MSVC
    // template-template defaulting hazard); the signature/value type is DEDUCED from the callable
    // via detail's function-traits — partial-explicit args CTAD cannot deliver. A generic lambda
    // or overloaded operator() has no single deducible signature and is rejected by
    // deducible_handler with "spell Sig explicitly".

    // Sig = Res(Req) is deduced from a (const Req&) -> expected<Res, error_code> handler.
    template<template<typename> class Family, typename Handler>
    auto serve(std::string_view fqn, Handler handler)
    {
        static_assert(detail::deducible_handler<Handler>,
                      "plexus: spell Sig explicitly — a generic lambda or overloaded call "
                      "operator has no single deducible signature; name the signature on the "
                      "endpoint");
        using Sig = detail::handler_signature_t<Handler>;
        return procedure<Sig, Family>{*this, fqn, std::move(handler)};
    }

    // A caller has no handler to deduce from, so the signature is spelled explicitly.
    template<typename Sig, template<typename> class Family>
    auto caller(std::string_view fqn)
    {
        return plexus::caller<Sig, Family>{*this, fqn};
    }

    // The value type T is deduced from a (const T&) or (const T&, message_info) callback.
    template<template<typename> class Family, typename Cb>
    auto subscribe(std::string_view topic, Cb cb)
    {
        static_assert(detail::deducible_handler<Cb>,
                      "plexus: spell the value type explicitly — a generic lambda or "
                      "overloaded call operator has no single deducible signature; name the "
                      "type on the endpoint");
        using T = detail::subscriber_value_t<Cb>;
        return subscriber<Family<T>>{*this, topic, std::move(cb)};
    }

    // A publisher has no callable to deduce from, so the codec is supplied as a finished type
    // (the pub/sub slots take finished codecs, not families).
    template<typename Codec>
    auto advertise(std::string_view topic, const typed_publisher_options &opts = {},
                   Codec codec = {})
    {
        return publisher<Codec>{*this, topic, opts, std::move(codec)};
    }

    // Decode one topic's samples and project each value to a printable record (CSV/JSONL/text per
    // opts.format). The projection defaults to the operator<< text floor; both codec and
    // projection live only in the returned handle — never the tap.
    template<typename Codec, typename Projection = no_projection>
    [[nodiscard]] auto log(std::string_view topic, const value_logger_options &opts,
                           Codec codec = {}, Projection projection = {})
    {
        return value_logger<Codec, Projection>(*this, topic, opts, std::move(codec),
                                               std::move(projection));
    }

    // The first-class consumer-sovereign capture verb (NOT the router() escape hatch): an RAII
    // recorder handle that registers its tap on the engine and deregisters before teardown. The
    // sink MUST outlive the handle; the drain rides this node's executor turns — no thread.
    [[nodiscard]] recorder<engine_type, Policy> make_recorder(io::recording::byte_sink &sink,
                                                              recorder_options          opts = {})
    {
        return recorder<engine_type, Policy>{m_engine, m_executor,      m_id,
                                             sink,     std::move(opts), m_wire_crypto_position};
    }
    // --------------------------------------------------------------------------------

    friend struct detail::peer_watch<node>;

private:
    // ---- Endpoint infrastructure (the topic->peer translation) ----------------------
    using object_entry = detail::object_entry;
    using subscription = detail::subscription;
    using peer_watch   = detail::peer_watch<node>;

    using registration_id = std::uint64_t;

    // Register a standing subscriber: mint its id, store it, and fan its demand to every known
    // peer (the re-fan reaches each peer discovered later). Returns the id retire keys on.
    registration_id
    register_subscriber_seam(std::string_view fqn, const io::subscriber_qos &qos,
                             plexus::detail::move_only_function<void(std::span<const std::byte>,
                                                                     const io::message_info &)>
                                                                   cb,
                             std::optional<std::uint64_t>          type_id = std::nullopt,
                             object_entry                          obj     = {},
                             std::optional<io::topic_capture_rule> capture = std::nullopt)
    {
        const registration_id rid = m_next_registration++;
        m_subscriptions.push_back(
                {rid, subscription{std::string{fqn}, qos, type_id, std::move(cb), std::move(obj)}});
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        // Thread the subscriber's own hint (the bilateral OR) and provision a co-host
        // subscriber-only default-geometry ring through the SAME path the publisher uses.
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
        provision_same_host_ring(fqn, subscriber_effective_bytes(qos), std::nullopt);
        m_engine.post_endpoint(
                fqn,
                {io::endpoint_edge::subscriber_registered, wire::fqn_topic_hash(fqn), type_id});
        for(const auto &peer : m_known_peers)
            m_engine.subscribe(peer, fqn, qos, io::locality::any, io::reliability_requirement::any,
                               type_id);
        return rid;
    }

    // Drop the demux entry and, when it was the LAST local subscriber for the fqn, unsubscribe
    // the topic from every peer it was fanned to.
    void retire_subscriber_seam(registration_id rid)
    {
        auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
                               [&](const auto &e) { return e.first == rid; });
        if(it == m_subscriptions.end())
            return;
        const std::string fqn = it->second.fqn;
        m_subscriptions.erase(it);
        if(!any_subscriber_for(fqn))
        {
            m_engine.post_endpoint(fqn,
                                   {io::endpoint_edge::subscriber_retired,
                                    wire::fqn_topic_hash(fqn), std::nullopt});
            for(const auto &peer : m_known_peers)
                m_engine.unsubscribe(peer, fqn);
        }
    }

    // Declare a publisher's topic and mint its gid. The DECLARATION persists for the node's life
    // (stable identity for subscriber correlation); the HANDLE's lifetime is distinct
    // (retire_publisher_seam posts the handle-drop edge).
    void declare_publisher_seam(std::string_view fqn, const topic_qos &qos,
                                bool                                  emit_source_identity,
                                std::optional<std::uint64_t>          type_id      = std::nullopt,
                                std::optional<shm::shm_geometry>  shm_geometry = std::nullopt,
                                std::optional<io::topic_capture_rule> capture      = std::nullopt)
    {
        m_engine.messages().declare(fqn, qos, type_id, emit_source_identity);
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.post_endpoint(
                fqn, {io::endpoint_edge::publisher_declared, wire::fqn_topic_hash(fqn), type_id});
        // Resolution order: per-topic ?: the shm member's own default ?: shipped. Producer-side
        // same-host-local, never wire-advertised, never RxO. An absent per-topic override defers to
        // each shm member's default_geometry().
        const std::size_t effective_bytes = io::effective_max(qos, m_max_message_bytes);
        provision_same_host_ring(fqn, effective_bytes, shm_geometry);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
    }

    // Post the HANDLE's drop edge. It does NOT tear down the persistent declaration (that lives
    // for the node's life by design) — it surfaces the handle's observable lifetime only.
    void retire_publisher_seam(std::string_view fqn)
    {
        m_engine.post_endpoint(
                fqn,
                {io::endpoint_edge::publisher_dropped, wire::fqn_topic_hash(fqn), std::nullopt});
    }

    // Record the topic's effective size + resolved geometry on the shm member, keyed by fqn,
    // BEFORE the dial/listen mints the ring. A composition with no shm member is a no-op. An absent
    // per-topic override resolves to each shm member's OWN default_geometry() (the member, not
    // node_options, owns the same-host ring default).
    void provision_same_host_ring(std::string_view fqn, std::size_t effective_bytes,
                                  std::optional<shm::shm_geometry> geom)
    {
        std::string key{fqn};
        for_each_shm_member([&](auto &m)
                            { m.set_topic_geometry(key, effective_bytes,
                                                   geom.value_or(m.default_geometry())); });
    }

    // A subscriber-only ring sizes to the subscriber's requested per-message max, else the node
    // default — the consumer-rescues-itself default-geometry ring.
    [[nodiscard]] std::size_t
    subscriber_effective_bytes(const io::subscriber_qos &qos) const noexcept
    {
        return qos.requested_max_message_bytes != 0
                ? static_cast<std::size_t>(qos.requested_max_message_bytes)
                : m_max_message_bytes;
    }

    // Source the upgrade policy from the shm member (it owns the default) and, when an shm member
    // exists, install its companion-ring MINT + RECEIVE gates (relocated to
    // detail::install_same_host_upgrade). No shm member = inert (no gate, no policy installed).
    void install_upgrade_coordinator()
    {
        for_each_shm_member(
                [this](auto &m)
                {
                    m_engine.on_upgrade_policy(m.upgrade_policy());
                    detail::install_same_host_upgrade(m_engine, m);
                });
    }

    // Apply fn to the shm member of the engine leaf, if one exists (the single borrowed transport
    // or whichever mux member exposes the provisioning verb). The if-constexpr capability check
    // makes a composition with no shm member a compile-time no-op.
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

    template<typename M>
    static constexpr bool has_topic_geometry = requires(M &m) {
        m.set_topic_geometry(std::string{}, std::size_t{}, shm::shm_geometry{});
    };

    // Resolve the FIRST connection-order peer with a complete session and route the call to it.
    // on_reply is fanned with an ENGAGED status + the resolved provider gid. With NO connected
    // provider, the completion is POSTED with an ABSENT status (the no_provider verdict, carried
    // out-of-band, never a fabricated rpc_status) — it never hangs, buffers, or queues.
    template<typename OnReply>
    void call_seam(std::string_view fqn, std::span<const std::byte> param, OnReply on_reply,
                   std::optional<std::chrono::nanoseconds> deadline)
    {
        for(const auto &peer : m_known_peers)
        {
            auto *session = m_engine.registry().session_for(peer);
            if(session == nullptr || !session->is_complete())
                continue;
            const std::optional<publisher_gid> provider{publisher_gid{peer, 0}};
            m_engine.procedures().call(
                    session->rpc_peer(), fqn, param,
                    [on_reply = std::move(on_reply),
                     provider](wire::rpc_status status, std::span<const std::byte> bytes) mutable
                    { on_reply(status, bytes, provider); },
                    deadline, session->session_id());
            return;
        }
        Policy::post(m_executor, [on_reply = std::move(on_reply)]() mutable
                     { on_reply(std::nullopt, {}, std::nullopt); });
    }

    // The local-uniqueness gate: the served-FQN set is checked BEFORE the forwarder is touched,
    // so a refused registration has ZERO side effects. A second LOCAL serve on one fqn throws
    // (a duplicate local provider is a programming error), closing the forwarder's own
    // silent-overwrite within-process hijack.
    void serve_procedure_seam(std::string_view fqn, io::handler_fn handler)
    {
        if(std::find(m_served_fqns.begin(), m_served_fqns.end(), fqn) != m_served_fqns.end())
            plexus::detail::fail_closed("plexus: a procedure is already served locally on this fqn");
        m_served_fqns.emplace_back(fqn);
        m_engine.procedures().serve(fqn, std::move(handler));
    }

    // Drop the handler and the served-FQN entry so a subsequent inbound call resolves
    // no_handler and the fqn is free to be served again.
    void retire_procedure_seam(std::string_view fqn)
    {
        m_engine.procedures().retire(fqn);
        std::erase(m_served_fqns, std::string{fqn});
    }

    // Record a peer to fan demand toward, dedup'd. Insertion order is awareness order — it also
    // feeds caller target resolution, so it stays endpoint-family-agnostic.
    void note_known_peer(const plexus::node_id &id)
    {
        if(std::find(m_known_peers.begin(), m_known_peers.end(), id) == m_known_peers.end())
            m_known_peers.push_back(id);
    }

    // Re-fan every standing demand toward one peer (idempotent per (peer, fqn)).
    void fan_demands_to(const plexus::node_id &id)
    {
        for(const auto &[rid, sub] : m_subscriptions)
            m_engine.subscribe(id, sub.fqn, sub.qos, io::locality::any,
                               io::reliability_requirement::any, sub.type_id);
    }

    // Fan a delivered frame to every callback registered for its fqn (both arities ride this one
    // path behind the 3-arg adapter). An fqn with no local subscriber is a silent no-op.
    void dispatch_message(std::string_view fqn, std::span<const std::byte> bytes,
                          const io::message_info &info)
    {
        for(auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                sub.cb(bytes, info);
    }

    // Fan a process-tier object handle to every typed subscriber on its fqn. A native_key MATCH
    // dispatches the concrete T for the callback's duration ONLY (the session releases the slot
    // right after); a MISMATCH is a COUNTED, warn-and-dropped event — NEVER a cast, the never-UB
    // backstop. A bytes-only subscription is reached through dispatch_message instead.
    void dispatch_object(std::string_view fqn, const io::object_carrier &carrier)
    {
        // Read the receive clock at most ONCE, and only if a matching subscriber wants
        // message_info; one that wants none is delivered a documented-0 stamp.
        std::uint64_t reception      = 0;
        bool          reception_read = false;

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

    // The per-subscriber message_info for an object dispatch. A subscriber that wants no info gets
    // a documented-0 stamp; one that wants it shares the at-most-once receive clock read across the
    // fan (reception/reception_read are threaded by reference so the clock is read on first need).
    io::message_info object_info_for(const subscription &sub, const io::object_carrier &carrier,
                                     std::uint64_t &reception, bool &reception_read) const
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
        return std::any_of(m_subscriptions.begin(), m_subscriptions.end(),
                           [&](const auto &e) { return e.second.fqn == fqn; });
    }

    // Fill the type-erased outbound-verb seam the endpoint handles capture. Each verb is a
    // captureless static lambda (a plain fn-ptr, zero alloc) recovering the node and forwarding
    // to the private *_seam VERBATIM, so the concrete Policy stays inside those bodies. The
    // inbound delivery path never crosses here.
    // NOLINTNEXTLINE(readability-function-size)
    io::endpoint_seam endpoint_seam_for() noexcept
    {
        io::endpoint_seam s{};
        s.ctx               = this;
        s.declare_publisher = [](void *ctx, std::string_view fqn, const topic_qos &qos, bool emit,
                                 std::optional<std::uint64_t> type_id, const void *geometry,
                                 std::optional<io::topic_capture_rule> capture)
        {
            std::optional<shm::shm_geometry> shm_geometry;
            if(geometry != nullptr)
                shm_geometry = *static_cast<const shm::shm_geometry *>(geometry);
            static_cast<node *>(ctx)->declare_publisher_seam(fqn, qos, emit, type_id, shm_geometry,
                                                             capture);
        };
        s.publish = [](void *ctx, std::string_view fqn, std::span<const std::byte> bytes)
        { static_cast<node *>(ctx)->m_engine.messages().publish(fqn, bytes); };
        s.publish_object = [](void *ctx, std::string_view fqn, const io::object_carrier &carrier,
                              io::encode_thunk encode)
        {
            static_cast<node *>(ctx)->m_engine.messages().publish_object(
                    fqn, carrier, [&] { return io::invoke(encode); });
        };
        s.register_subscriber = [](void *ctx, std::string_view fqn, const io::subscriber_qos &qos,
                                   io::bytes_cb cb, std::optional<std::uint64_t> type_id,
                                   const void *native_key, io::object_dispatch dispatch,
                                   std::optional<io::topic_capture_rule> capture) -> registration_id
        {
            return static_cast<node *>(ctx)->register_subscriber_seam(
                    fqn, qos, std::move(cb), type_id, object_entry{native_key, std::move(dispatch)},
                    capture);
        };
        s.retire_subscriber = [](void *ctx, registration_id rid)
        { static_cast<node *>(ctx)->retire_subscriber_seam(rid); };
        s.retire_publisher = [](void *ctx, std::string_view fqn)
        { static_cast<node *>(ctx)->retire_publisher_seam(fqn); };
        s.serve_procedure = [](void *ctx, std::string_view fqn, io::handler_fn handler)
        { static_cast<node *>(ctx)->serve_procedure_seam(fqn, std::move(handler)); };
        s.retire_procedure = [](void *ctx, std::string_view fqn)
        { static_cast<node *>(ctx)->retire_procedure_seam(fqn); };
        s.call = [](void *ctx, std::string_view fqn, std::span<const std::byte> param,
                    io::on_reply_fn on_reply, std::optional<std::chrono::nanoseconds> deadline)
        { static_cast<node *>(ctx)->call_seam(fqn, param, std::move(on_reply), deadline); };
        return s;
    }
    // --------------------------------------------------------------------------------

    static io::handshake_fsm_config make_fsm_cfg(const plexus::node_id &id,
                                                 const node_options    &opts)
    {
        return io::handshake_fsm_config{
                .self_id                  = id,
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

    // The engine's transport leaf: the one borrowed transport, or the node-owned mux glue when
    // composing several.
    engine_transport &engine_leaf(Transports &...transports) noexcept
    {
        if constexpr(sizeof...(Transports) == 1)
            return std::get<0>(std::tie(transports...));
        else
            return m_glue;
    }

    // Advertise the card under the service endpoint carrying the node's reachable host (the first
    // listen's host; empty until the first listen). Port keys ride the card metadata. A real mDNS
    // backend fills the host from the resolved record; over static_discovery the node supplies it.
    void advertise_card()
    {
        m_disc.advertise({m_service_name, io::endpoint{"", m_host},
                          discovery::assemble_contact_card(m_id, m_listens)});
    }

    // Parse a browsed card into an awareness entry, reject-on-failure (the card is untrusted
    // multicast input): a malformed/missing node_id, a self card, a host-less address, or no
    // usable port key each abort WITHOUT a note_peer. The first valid port key in card order wins.
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

    plexus::node_id                             m_id;
    executor_type                               m_executor;
    discovery::discovery                       &m_disc;
    // The node owns its inert default logger and binds m_logger / the engine to it by reference
    // when no logger is supplied — declared immediately ABOVE m_logger so it is fully constructed
    // before resolve_logger binds either reference in the init-list. One owned sink, no global.
    log::null_logger                            m_default_logger;
    log::logger                                &m_logger;
    std::string                                 m_service_name;
    std::string                                 m_host;
    std::vector<discovery::listening_transport> m_listens;

    // The node-level per-message size default the declare path resolves the effective geometry
    // against. The same-host ring geometry + slab-ceiling defaults live on the shm member itself.
    std::size_t m_max_message_bytes;

    // Retained from node_options.wire so make_recorder can stamp it into the recorder's stream
    // preamble (a recording-only fact, never on the live wire).
    wire_crypto_position m_wire_crypto_position;

    // Constructed in place from the borrowed leaves: the non-movable mux is never a prvalue, so
    // the empty single-transport glue keeps its zero-size overlap under [[no_unique_address]].
    [[no_unique_address]] detail::node_mux_glue<Transports...> m_glue;

    // The engine's transport leaf, decoupled from the member pack's order/types.
    engine_transport &m_leaf;

    // Declared BEFORE the engine (which captures &m_peer_watch and a `this`-bound route) so the
    // engine is destroyed FIRST, leaving no dangling observer/route reference during teardown.
    peer_watch                                            m_peer_watch{*this};
    std::vector<plexus::node_id>                          m_known_peers;
    std::vector<std::pair<registration_id, subscription>> m_subscriptions;
    std::vector<std::string>                              m_served_fqns;
    registration_id                                       m_next_registration{1};
    std::size_t                                           m_object_dispatch_mismatch{};

    engine_type m_engine;
};

}

#endif
