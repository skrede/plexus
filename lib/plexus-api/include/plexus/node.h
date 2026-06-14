#ifndef HPP_GUARD_PLEXUS_NODE_H
#define HPP_GUARD_PLEXUS_NODE_H

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

#include "plexus/io/shm/shm_mux_member.h"

#include "plexus/log/logger.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/muxify.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/address_parse.h"
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
#include <stdexcept>
#include <algorithm>
#include <functional>
#include <type_traits>
#include <string_view>

namespace plexus {

namespace detail {

// One transport composes directly (the concrete Policy, zero channel indirection);
// two or more compose through a node-owned multiplexing_transport over the erased
// muxify<Policy>. The selection is LAZY (a `::type` indirection, not std::conditional_t
// on the types) so the multiplexing_transport branch is NEVER instantiated for a
// single transport — instantiating it eagerly would fail mux_member for a leaf whose
// concrete-channel completion shape does not satisfy the multiplexer's erased one.
template <typename Policy, typename... Transports>
using node_engine_policy =
    std::conditional_t<sizeof...(Transports) == 1, Policy, muxify<Policy>>;

// The single-transport node carries no composition glue. An empty member under
// [[no_unique_address]] keeps it zero-overhead; the multi-transport node holds the
// node-owned multiplexing_transport built from the borrowed leaves.
struct no_mux_glue {};

template <bool Single, typename... Transports>
struct mux_selection
{
    using transport = io::multiplexing_transport<Transports...>;
    using glue = io::multiplexing_transport<Transports...>;
};

template <typename... Transports>
struct mux_selection<true, Transports...>
{
    using transport = std::tuple_element_t<0, std::tuple<Transports...>>;
    using glue = no_mux_glue;
};

template <typename... Transports>
using node_engine_transport =
    typename mux_selection<sizeof...(Transports) == 1, Transports...>::transport;

template <typename... Transports>
using node_mux_glue =
    typename mux_selection<sizeof...(Transports) == 1, Transports...>::glue;

// FNV-1a 64 over the name, run twice with distinct offset baselines, folding the two
// 8-byte digests into the 16-byte node_id. Deterministic and platform-stable (the
// wire::fqn_topic_hash precedent): the SAME name yields the SAME identity. That is an
// intentional property of the OPT-IN name overload, not a collision trap — a caller
// who wants distinct identities for equal names supplies the node_id verbatim.
inline node_id hash_node_id(std::string_view name) noexcept
{
    constexpr std::uint64_t k_prime = 1099511628211ull;
    constexpr std::uint64_t k_basis_lo = 1469598103934665603ull;
    constexpr std::uint64_t k_basis_hi = 0x9e3779b97f4a7c15ull;
    std::uint64_t lo = k_basis_lo;
    std::uint64_t hi = k_basis_hi;
    for(char c : name)
    {
        const auto byte = static_cast<std::uint64_t>(static_cast<unsigned char>(c));
        lo = (lo ^ byte) * k_prime;
        hi = (hi ^ byte) * k_prime;
    }
    node_id id{};
    for(int i = 0; i < 8; ++i)
    {
        id[i] = static_cast<std::byte>((lo >> (8 * i)) & 0xff);
        id[8 + i] = static_cast<std::byte>((hi >> (8 * i)) & 0xff);
    }
    return id;
}

}

// The public name-hash helper: derive a node_id from a name deterministically, so the
// SAME name yields the SAME identity. An intentional OPT-IN property, never the default
// (the verbatim node_id is the primary identity path) — equal to the id() of a node
// constructed with the same name string.
inline node_id node_id_from_name(std::string_view name) noexcept
{
    return detail::hash_node_id(name);
}

struct typed_publisher_options;

template <typename Codec = void>
class publisher;

template <typename Codec = void>
class subscriber;

template <typename T> struct no_codec;

template <typename Sig = void,
          template <typename> class CReq = no_codec,
          template <typename> class CRes = CReq>
class caller;

template <typename Sig = void,
          template <typename> class CReq = no_codec,
          template <typename> class CRes = CReq>
class procedure;

// The consumable public surface: a node composes a routing_engine over an injected
// substrate. Policy is explicit (the plexus convention); the trailing transport pack
// is deduced. A single transport binds Policy directly; two or more bind muxify<Policy>
// over a node-owned multiplexing_transport. Omitting any substrate element — executor,
// discovery, the transports, or the options — is a COMPILE ERROR: there is no owning
// convenience overload.
//
// LIFETIME: the node BORROWS its executor, discovery, and transport leaves by
// reference and OWNS only the engine and (for >1 transport) the multiplexing glue.
// Every engine callback is POSTED on the borrowed executor and captures `this`. The
// borrowed substrate MUST outlive the node, and the executor MUST be drained (or
// quiesced) before the node is destroyed — the same structural single-owner
// discipline the engine relies on. There is no per-callback liveness guard (a guard
// reading a member would itself touch dead `this`); the owner sequences teardown.
// The node pins `this` into those callbacks, so it is non-copyable and non-movable.
//
// Exceeds the navigable line cap: the facade proper is one cohesive class (ctor, the
// member endpoint factories, and the private outbound seams) whose members capture
// `this` and reach node-private state, so they cannot relocate without becoming a
// behavior-changing rewrite. The relocatable internals already live in detail/.
template <typename Policy, typename... Transports>
    requires plexus::Policy<Policy>
          && (sizeof...(Transports) >= 1)
          && (sizeof...(Transports) == 1
                  ? io::transport_backend<
                        std::tuple_element_t<0, std::tuple<Transports..., void>>, Policy>
                  : (io::mux_member<Transports> && ...))
class node
{
    // The four endpoint handles are the ONLY construction path for an endpoint: the
    // registration/retire seams below are PRIVATE, reachable solely through these
    // friends, so there is no public node.publish / node.subscribe / declare_* factory.
    template <typename C> friend class publisher;
    template <typename C> friend class subscriber;
    template <typename S, template <typename> class Cq, template <typename> class Cs>
    friend class caller;
    template <typename S, template <typename> class Cq, template <typename> class Cs>
    friend class procedure;

public:
    using executor_type = typename Policy::executor_type;
    using engine_policy = detail::node_engine_policy<Policy, Transports...>;
    using engine_transport = detail::node_engine_transport<Transports...>;
    using engine_type =
        io::routing_engine<engine_policy, engine_transport>;

    // The node_id is taken VERBATIM: plexus compares the identity, never mints or
    // interprets it.
    node(executor_type executor, discovery::discovery &disc, const plexus::node_id &id,
         Transports &...transports, const node_options &opts)
        : m_id(id)
        , m_executor(executor)
        , m_disc(disc)
        , m_logger(resolve_logger(opts))
        , m_service_name(opts.name.empty() ? io::node_name_of(id) : opts.name)
        , m_max_message_bytes(opts.max_message_bytes)
        , m_shm_geometry(opts.shm_geometry)
        , m_max_ring_slab_bytes(opts.max_ring_slab_bytes)
        , m_glue(make_glue(transports...))
        , m_leaf(engine_leaf(transports...))
        , m_engine(m_leaf, executor, make_fsm_cfg(id, opts),
                   opts.handshake_timeout, opts.reconnect, opts.redial_seed,
                   opts.dial_eagerly, resolve_logger(opts), opts.max_message_bytes)
    {
        // The fqn-to-callback demux: install the node-shared receive route ONCE, before
        // any session is built (the route's set-before-listen contract). Every delivered
        // data frame fans to every callback locally registered for its fqn.
        m_engine.on_message_route(
            [this](std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info)
            { dispatch_message(fqn, bytes, info); });
        // The object-lane demux beside the byte route: a process-tier object handle fans
        // to every typed subscriber on its fqn, native-key-checked (never a cast on a
        // mismatch). The route is a READ-ONLY sub-callback — the session owns the release.
        m_engine.on_object_route(
            [this](std::string_view fqn, const io::object_carrier &carrier)
            { dispatch_object(fqn, carrier); });
        // Observe peer-ready edges so a standing demand re-fans to a peer that becomes
        // ready AFTER the demand was registered (the late-join half on the sub side).
        m_engine.add_observer(m_peer_watch);
        // Advertise the (initially port-less) contact card at construction so a dial-only
        // node is discoverable from birth, and browse to awareness. These run
        // synchronously in the ctor turn (not posted) — they install state before any
        // session exists, the set-before-listen contract. The browse handler also re-fans
        // every standing demand toward a newly noted peer (the late-join half).
        advertise_card();
        m_disc.browse([this](const discovery::service_info &peer) { note_from_card(peer); });
        // Apply the node-level per-ring slab ceiling to the shm member once: it bounds
        // every same-host ring this node mints. A composition without an shm member is a
        // no-op (the capability check below fails).
        apply_shm_slab_ceiling(m_max_ring_slab_bytes);
    }

    // The name-hash overload (OPT-IN): derives the node_id from the name so the SAME
    // name yields the SAME identity (detail::hash_node_id). An intentional property
    // of this overload, never the default — the verbatim ctor above is the primary.
    node(executor_type executor, discovery::discovery &disc, std::string_view name,
         Transports &...transports, const node_options &opts)
        : node(executor, disc, detail::hash_node_id(name), transports..., opts)
    {
    }

    node(const node &) = delete;
    node &operator=(const node &) = delete;
    node(node &&) = delete;
    node &operator=(node &&) = delete;

    // Forward the bind to the engine, then append this transport's {scheme, port} to
    // the live contact card and re-advertise. PRECONDITION: an advertised listen requires
    // an EXPLICIT port in ep.address ("host:port") — a port-0 auto-assign cannot be
    // advertised, because the transport_backend concept exposes no bound-port accessor. A
    // missing/unparsable port binds the engine but advertises NO port key for it.
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

    // The node's own identity: verbatim from the ctor, or the name-hash derivation. The
    // authoritative self identity the engine handshakes with.
    const plexus::node_id &id() const noexcept { return m_id; }

    // Escape hatches: the live engine objects, for advanced peer-level work the
    // topic-level public verbs deliberately hide.
    engine_type &router() noexcept { return m_engine; }
    const engine_type &router() const noexcept { return m_engine; }
    auto &message_forwarder() noexcept { return m_engine.messages(); }
    executor_type executor() const noexcept { return m_executor; }

    // The count of object-lane deliveries dropped at the demux because the carrier's
    // process-local type witness did not match a tag-equal subscriber's — the never-UB
    // backstop's readable signal (never a cast, never silent).
    [[nodiscard]] std::size_t object_dispatch_mismatch() const noexcept
    {
        return m_object_dispatch_mismatch;
    }

    // ---- Member factories (partial-explicit deduction) ------------------------------
    //
    // The codec FAMILY is spelled explicitly (Family is NON-defaulted, sidestepping the
    // MSVC template-template defaulting hazard); the signature/value type is DEDUCED from
    // the concrete callable via detail's function-traits. A function template delivers what
    // CTAD cannot — partial-explicit template arguments with the rest deduced — so the
    // default RPC/sub experience is `node.serve<pair_codec>("div", handler)`.
    //
    // A generic lambda or an overloaded-operator() handler has no single deducible
    // signature; the deducible_handler static_assert rejects it with "spell Sig explicitly"
    // and the caller names the signature on the endpoint instead.

    // Serve a typed procedure: Sig = Res(Req) is deduced from a
    // (const Req&) -> expected<Res, error_code> handler; the family expands to
    // Family<Req> / Family<Res>.
    template <template <typename> class Family, typename Handler>
    auto serve(std::string_view fqn, Handler handler)
    {
        static_assert(detail::deducible_handler<Handler>,
                      "plexus: spell Sig explicitly — a generic lambda or overloaded call "
                      "operator has no single deducible signature; name the signature on the "
                      "endpoint");
        using Sig = detail::handler_signature_t<Handler>;
        return procedure<Sig, Family>{*this, fqn, std::move(handler)};
    }

    // A typed calling endpoint. A caller has no handler to deduce from, so the signature
    // is spelled explicitly alongside the family: node.caller<Res(Req), pair_codec>("div").
    template <typename Sig, template <typename> class Family>
    auto caller(std::string_view fqn)
    {
        return plexus::caller<Sig, Family>{*this, fqn};
    }

    // Subscribe a typed topic: the value type T is deduced from a (const T&) or
    // (const T&, message_info) callback; the family expands to Family<T>.
    template <template <typename> class Family, typename Cb>
    auto subscribe(std::string_view topic, Cb cb)
    {
        static_assert(detail::deducible_handler<Cb>,
                      "plexus: spell the value type explicitly — a generic lambda or "
                      "overloaded call operator has no single deducible signature; name the "
                      "type on the endpoint");
        using T = detail::subscriber_value_t<Cb>;
        return subscriber<Family<T>>{*this, topic, std::move(cb)};
    }

    // Advertise a typed topic. A publisher has no callable to deduce from, so the codec is
    // supplied as a finished type (the pub/sub slots take finished codecs, not families):
    // node.advertise<reading_codec>("telemetry").
    template <typename Codec>
    auto advertise(std::string_view topic, const typed_publisher_options &opts = {}, Codec codec = {})
    {
        return publisher<Codec>{*this, topic, opts, std::move(codec)};
    }
    // --------------------------------------------------------------------------------

    friend struct detail::peer_watch<node>;

private:
    // ---- Endpoint infrastructure (the topic->peer translation) ----------------------
    using object_entry = detail::object_entry;
    using subscription = detail::subscription;
    using peer_watch   = detail::peer_watch<node>;

    // The standing-demand table doubling as the demux map. register_subscriber_seam
    // fans engine.subscribe to every known peer (now) and the re-fan reaches each peer
    // discovered/ready later; dispatch_message walks it on the receive path.
    using registration_id = std::uint64_t;

    // Register a standing subscriber: mint its id, store it, and fan its demand to every
    // currently known peer. Returns the id the retire seam keys on.
    registration_id register_subscriber_seam(
        std::string_view fqn, const io::subscriber_qos &qos,
        plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb,
        std::optional<std::uint64_t> type_id = std::nullopt,
        object_entry obj = {})
    {
        const registration_id rid = m_next_registration++;
        m_subscriptions.push_back(
            {rid, subscription{std::string{fqn}, qos, type_id, std::move(cb), std::move(obj)}});
        for(const auto &peer : m_known_peers)
            m_engine.subscribe(peer, fqn, qos, io::locality::any,
                               io::reliability_requirement::any, type_id);
        return rid;
    }

    // Retire a standing subscriber: drop its demux entry and, when it was the LAST local
    // subscriber for the fqn, unsubscribe the topic from every peer it was fanned to.
    void retire_subscriber_seam(registration_id rid)
    {
        auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(),
                               [&](const auto &e) { return e.first == rid; });
        if(it == m_subscriptions.end())
            return;
        const std::string fqn = it->second.fqn;
        m_subscriptions.erase(it);
        if(!any_subscriber_for(fqn))
            for(const auto &peer : m_known_peers)
                m_engine.unsubscribe(peer, fqn);
    }

    // Declare a publisher's topic and mint its gid. The producer-side declaration
    // persists for the node's life so the endpoint counter stays stable and is NEVER
    // reused — a dropped publisher stops publishing, but its declaration is not torn
    // down, so retire is a no-op (no resource to reclaim, and removing it would risk a
    // gid remint). The handle drives publish directly.
    void declare_publisher_seam(std::string_view fqn, const topic_qos &qos, bool emit_source_identity,
                                std::optional<std::uint64_t> type_id = std::nullopt,
                                std::optional<io::shm::shm_geometry> shm_geometry = std::nullopt)
    {
        m_engine.messages().declare(fqn, qos, type_id, emit_source_identity);
        // Resolution order per-topic ?: node-default ?: shipped: the per-topic geometry
        // when declared, else the node-level default; the effective per-message size is
        // the topic override when set, else the node default. The provisioning is a
        // producer-side same-host-local value — never wire-advertised, never RxO.
        const io::shm::shm_geometry geom = shm_geometry.value_or(m_shm_geometry);
        const std::size_t effective_bytes = io::effective_max(qos, m_max_message_bytes);
        provision_same_host_ring(fqn, effective_bytes, geom);
    }

    // Provision the declaring topic's same-host ring geometry on the shm member (if the
    // composition has one): the publisher records its effective size + resolved geometry,
    // keyed by fqn, BEFORE the dial/listen mints the ring. A composition without an shm
    // member is a no-op. Producer-side, never wire-advertised.
    void provision_same_host_ring(std::string_view fqn, std::size_t effective_bytes,
                                  const io::shm::shm_geometry &geom)
    {
        std::string key{fqn};
        for_each_shm_member([&](auto &m) { m.set_topic_geometry(key, effective_bytes, geom); });
    }

    void apply_shm_slab_ceiling(std::uint64_t bytes)
    {
        for_each_shm_member([&](auto &m) { m.set_max_ring_slab_bytes(bytes); });
    }

    // Apply fn to the shm member of the engine transport leaf, if one exists. The leaf is
    // either the node-owned mux glue (apply fn to whichever member exposes the producer
    // provisioning verb) or a single borrowed transport (apply fn directly if it is the
    // shm member). The capability check is an if-constexpr on the member type, so a
    // composition with no shm member is a compile-time no-op.
    template <typename F>
    void for_each_shm_member(F &&fn)
    {
        if constexpr(sizeof...(Transports) == 1)
        {
            if constexpr(has_topic_geometry<engine_transport>)
                fn(m_leaf);
        }
        else
        {
            m_leaf.for_each_member([&](auto &m) {
                using M = std::remove_reference_t<decltype(m)>;
                if constexpr(has_topic_geometry<M>)
                    fn(m);
            });
        }
    }

    template <typename M>
    static constexpr bool has_topic_geometry =
        requires(M &m) { m.set_topic_geometry(std::string{}, std::size_t{}, io::shm::shm_geometry{}); };

    // The caller seam: resolve the FIRST connection-order peer with a complete session
    // and route the call to it directly through the procedure forwarder (carrying the
    // per-call deadline override and the live session epoch). on_reply is fanned with an
    // ENGAGED wire status + the resolved provider's gid (its node_id half; the
    // endpoint_counter half stays the documented absent 0 — the rpc_response wire does
    // not echo it) so the caller attributes the reply. With NO connected provider, the
    // completion is POSTED on the borrowed executor with an ABSENT status (the
    // no_provider verdict — never on the wire, so it is carried out-of-band, not as a
    // fabricated rpc_status) and never touches the forwarder. The engine silently drops a
    // pre-completion call; the facade completes the errc itself — it never hangs,
    // buffers, or queues.
    template <typename OnReply>
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
                [on_reply = std::move(on_reply), provider](
                    wire::rpc_status status, std::span<const std::byte> bytes) mutable
                { on_reply(status, bytes, provider); },
                deadline, session->session_id());
            return;
        }
        Policy::post(m_executor, [on_reply = std::move(on_reply)]() mutable
                     { on_reply(std::nullopt, {}, std::nullopt); });
    }

    // Serve a LOCAL procedure (the local-uniqueness gate). The served-FQN set is checked
    // BEFORE the forwarder is touched, so a refused registration has ZERO side effects: a
    // second LOCAL serve on one fqn throws std::logic_error (a constructor has no
    // error-return channel, and a duplicate local provider is a programming error) and
    // leaves the first handler serving. The forwarder's own serve() would silently
    // overwrite — this facade gate closes that within-process hijack-by-overwrite.
    void serve_procedure_seam(std::string_view fqn, io::handler_fn handler)
    {
        if(std::find(m_served_fqns.begin(), m_served_fqns.end(), fqn) != m_served_fqns.end())
            throw std::logic_error("plexus: a procedure is already served locally on this fqn");
        m_served_fqns.emplace_back(fqn);
        m_engine.procedures().serve(fqn, std::move(handler));
    }

    // Retire a LOCAL procedure: drop the forwarder handler and the served-FQN entry so a
    // subsequent inbound call resolves the existing rpc_status::no_handler, and the fqn is
    // free to be served again.
    void retire_procedure_seam(std::string_view fqn)
    {
        m_engine.procedures().retire(fqn);
        std::erase(m_served_fqns, std::string{fqn});
    }

    // Record a peer the node may fan demand toward (browse-noted or ready), dedup'd. The
    // insertion order is connection/awareness order — it also feeds caller target
    // resolution, so it stays endpoint-family-agnostic.
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

    // The demux: fan a delivered frame to every callback registered for its fqn. A
    // 2-arg subscriber stored its callback behind a 3-arg adapter, so both arities ride
    // this one path. An fqn with no local subscriber is a silent no-op.
    void dispatch_message(std::string_view fqn, std::span<const std::byte> bytes,
                          const io::message_info &info)
    {
        for(auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                sub.cb(bytes, info);
    }

    // The object-lane demux: fan a process-tier object handle to every typed subscriber
    // on its fqn. A native_key MATCH dispatches the concrete T to the typed callback for
    // the callback's duration ONLY (the carrier's slot is released by the session right
    // after this route returns). A native_key MISMATCH (a tag-equal carrier of a
    // different C++ type) is a COUNTED, warn-and-dropped event — NEVER a cast, the
    // never-UB backstop. A subscription with no object entry (bytes-only) is skipped: the
    // byte fallback reaches it through dispatch_message instead.
    void dispatch_object(std::string_view fqn, const io::object_carrier &carrier)
    {
        // Demand-driven reception stamp: read the receive clock at most ONCE, and only if
        // some matching subscriber actually wants message_info (its local arity demand). A
        // subscriber that wants no info is delivered a documented-0 stamp (both source and
        // reception zeroed) and never triggers the clock read. publication_sequence and
        // from_intra_process are arity-independent and always honest; source_identity is
        // absent (the object lane carries no gid).
        std::uint64_t reception = 0;
        bool reception_read = false;

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
            io::message_info info{};
            info.publication_sequence = carrier.sequence;
            info.from_intra_process   = true;
            if(sub.qos.wants_message_info)
            {
                if(!reception_read)
                {
                    reception = wire::now_timestamp_ns();
                    reception_read = true;
                }
                info.source_timestamp    = carrier.source_timestamp;
                info.reception_timestamp = reception;
            }
            sub.obj.dispatch(carrier, info);
        }
    }

    bool any_subscriber_for(std::string_view fqn) const
    {
        return std::any_of(m_subscriptions.begin(), m_subscriptions.end(),
                           [&](const auto &e) { return e.second.fqn == fqn; });
    }

    // Fill the type-erased outbound-verb seam the endpoint handles capture at
    // construction. ctx is this node; each verb is a captureless static lambda
    // (converts to a plain fn-ptr, zero alloc) recovering the node and forwarding to the
    // private *_seam member VERBATIM — the concrete Policy stays inside those bodies
    // (Policy::post in call_seam, the logic_error throw in serve_procedure_seam both
    // propagate through the trampoline). The inbound delivery path never crosses here.
    io::endpoint_seam endpoint_seam_for() noexcept
    {
        io::endpoint_seam s{};
        s.ctx = this;
        s.declare_publisher = [](void *ctx, std::string_view fqn, const topic_qos &qos,
                                 bool emit, std::optional<std::uint64_t> type_id,
                                 std::optional<io::shm::shm_geometry> shm_geometry)
        { static_cast<node *>(ctx)->declare_publisher_seam(fqn, qos, emit, type_id, shm_geometry); };
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
                                   const void *native_key, io::object_dispatch dispatch) -> registration_id
        {
            return static_cast<node *>(ctx)->register_subscriber_seam(
                fqn, qos, std::move(cb), type_id,
                object_entry{native_key, std::move(dispatch)});
        };
        s.retire_subscriber = [](void *ctx, registration_id rid)
        { static_cast<node *>(ctx)->retire_subscriber_seam(rid); };
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

    static io::handshake_fsm_config make_fsm_cfg(const plexus::node_id &id, const node_options &opts)
    {
        return io::handshake_fsm_config{
            .self_id = id,
            .version_major = opts.handshake.version_major,
            .version_minor = opts.handshake.version_minor,
            .compatible_version_major = opts.handshake.compatible_version_major,
            .compatible_version_minor = opts.handshake.compatible_version_minor,
            .local_fingerprint = opts.handshake.local_fingerprint,
            .attach_policy = opts.attach_policy};
    }

    static log::logger &resolve_logger(const node_options &opts)
    {
        return opts.logger != nullptr ? *opts.logger : io::shared_null_logger();
    }

    // Whether a member exposes the same-host ring-acquire probe (the shm member). The
    // node reads it to install the same-host preference hook only when a composition
    // actually carries shared memory; a composition without it keeps the default
    // first-candidate hook unchanged.
    template <typename M>
    static constexpr bool has_can_acquire =
        requires(M &m) { { m.can_acquire(std::declval<const io::endpoint &>()) } -> std::convertible_to<bool>; };

    static constexpr bool any_shm_member = (has_can_acquire<Transports> || ...);

    // The single-transport node has no glue (an empty member); the multi-transport node
    // constructs the multiplexing_transport from the borrowed leaves. When the pack
    // carries a shared-memory member, the node installs the same-host preference hook
    // (prefer shm when the ring acquires, else AF_UNIX) so the local tier's >1 candidate
    // resolves by the runtime acquire rather than positional order. The hook is
    // can_acquire-gated, which is mode-aware: a wire_fallback topic declines the ring so
    // its same-host channel is the wire (the fail-safe — a too-large message always has a
    // reliable channel), while the two reliable-ring modes prefer shm. A composition with
    // no shm member keeps the default first-candidate hook.
    static detail::node_mux_glue<Transports...> make_glue(Transports &...transports)
    {
        if constexpr(sizeof...(Transports) == 1)
            return detail::no_mux_glue{};
        else if constexpr(any_shm_member)
            return io::multiplexing_transport<Transports...>{
                transports..., io::transport_selector{}, shm_preference_hook(transports...)};
        else
            return io::multiplexing_transport<Transports...>{transports...};
    }

    // Build the same-host preference hook over whichever borrowed member exposes the ring
    // acquire probe. The fold visits each leaf and captures the shm member by reference
    // (it outlives the node-owned glue); prefer_shm_hook reads can_acquire per same-host
    // dial. Precondition (any_shm_member): exactly the shm-bearing composition reaches here.
    static io::selection_hook shm_preference_hook(Transports &...transports)
    {
        io::selection_hook hook = io::first_candidate{};
        (void)((has_can_acquire<Transports>
                    ? (hook = io::shm::prefer_shm_hook(transports), true)
                    : false) || ...);
        return hook;
    }

    // The engine's transport leaf: the one borrowed transport directly, or the
    // node-owned multiplexing glue when composing several. The single-transport leaf
    // is the caller's own transport (the engine borrows it); the multi case borrows
    // the node-owned glue.
    engine_transport &engine_leaf(Transports &...transports) noexcept
    {
        if constexpr(sizeof...(Transports) == 1)
            return std::get<0>(std::tie(transports...));
        else
            return m_glue;
    }

    // Advertise the card under the service endpoint carrying the node's own reachable
    // host (the first listen's host; empty until the first listen). The port keys ride
    // the card metadata; the service endpoint carries the host a browser resolves the
    // card against. A real mDNS backend fills the host from the resolved record — over
    // a verbatim-carrying static_discovery the node supplies it from its own bind.
    void advertise_card()
    {
        m_disc.advertise({m_service_name, io::endpoint{"", m_host},
                          discovery::assemble_contact_card(m_id, m_listens)});
    }

    // Parse a browsed card into an awareness entry. Every step is reject-on-
    // failure (the card is untrusted multicast input): a malformed/missing node_id, a
    // self card, an address without a host, or no usable port key each abort WITHOUT a
    // note_peer. The first valid "plexus/<scheme>/port" key in card order wins; the
    // resolved SRV endpoint port is IGNORED (a port-less advertisement leaves it 0).
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
    log::logger &m_logger;
    std::string m_service_name;
    std::string m_host;
    std::vector<discovery::listening_transport> m_listens;

    // The node-level size + same-host ring defaults a publisher with no per-topic
    // override resolves against (per-topic ?: these node defaults ?: shipped constant).
    // Held so the declare path can resolve the effective geometry and provision the
    // same-host ring; producer-side only, never wire-advertised.
    std::size_t m_max_message_bytes;
    io::shm::shm_geometry m_shm_geometry;
    std::uint64_t m_max_ring_slab_bytes;

    [[no_unique_address]] detail::node_mux_glue<Transports...> m_glue;

    // The engine's transport leaf (the borrowed single transport, or the node-owned
    // mux glue): held so the declare path can provision the same-host ring geometry on
    // the shm member through it, decoupled from the member pack's order/types.
    engine_transport &m_leaf;

    // The endpoint state is declared BEFORE the engine so the engine — which captures
    // &m_peer_watch through add_observer and a `this`-bound route — is destroyed FIRST,
    // leaving no dangling observer/route reference during teardown.
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
