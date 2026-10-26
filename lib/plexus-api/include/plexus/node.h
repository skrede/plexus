#ifndef HPP_GUARD_PLEXUS_NODE_H
#define HPP_GUARD_PLEXUS_NODE_H

#include "plexus/recorder.h"
#include "plexus/node_options.h"
#include "plexus/target_profile.h"
#include "plexus/graph_change_handle.h"
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
#include "plexus/io/upgrade_channel.h"
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
#include "plexus/detail/node_close.h"
#include "plexus/detail/fail_closed.h"
#include "plexus/detail/topic_sweep.h"
#include "plexus/detail/address_parse.h"
#include "plexus/detail/node_identity.h"
#include "plexus/detail/node_internals.h"
#include "plexus/detail/node_rpc_seams.h"
#include "plexus/detail/node_same_host.h"
#include "plexus/detail/function_traits.h"
#include "plexus/detail/node_self_route.h"
#include "plexus/detail/call_origination.h"
#include "plexus/detail/node_relay_posture.h"
#include "plexus/detail/node_subscriptions.h"
#include "plexus/detail/node_discovery_glue.h"
#include "plexus/detail/node_upgrade_wiring.h"
#include "plexus/detail/node_object_dispatch.h"
#include "plexus/detail/node_self_carrier_sync.h"

#include <span>
#include <tuple>
#include <memory>
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

// LIFETIME: the node BORROWS its executor, discovery, and transport leaves, which MUST outlive it, and OWNS only the
// engine and (for >1 transport) the multiplexing glue. Its posts and waits capture `this`, so it is non-copyable and
// non-movable. A closed node may be destroyed inside or after its on_closed while the executor keeps running; a node
// never closed is destroyed only with its executor not run again.
template<typename Profile, typename... Transports>
    requires detail::target_profile<Profile> && (sizeof...(Transports) >= 1) &&
        (sizeof...(Transports) == 1 ? io::transport_backend<std::tuple_element_t<0, std::tuple<Transports..., void>>, typename detail::profile_traits<Profile>::policy> : (io::mux_member<Transports> && ...))
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
    using policy_type      = typename detail::profile_traits<Profile>::policy;
    using executor_type    = typename policy_type::executor_type;
    using engine_policy    = detail::node_engine_policy<policy_type, Transports...>;
    using engine_transport = detail::node_engine_transport<Transports...>;
    // The engine owns forward-splice selection: a relay profile (a real peer_report_emitter) threads the
    // real forward_splice keyed on the ENGINE policy — a mux relay's engine channel is
    // polymorphic_byte_channel, so keying on the profile policy would mis-type against it — while a
    // non-relay profile (the null emitter) threads the byte-identical null twin that reaches no
    // forwarding-send code. The emitter pivot is the single relay-vs-leaf discriminator the profile carries.
    using engine_forward_splice = std::conditional_t<std::is_same_v<typename detail::profile_traits<Profile>::peer_report_emitter, io::null_peer_report_emitter>,
                                                     io::null_forward_splice, io::forward_splice<engine_policy>>;
    // Storage is threaded off the profile, never off engine_policy/muxify — muxify re-exports only
    // the mechanism members, so reading storage through it silently reverts to the heap twins.
    using engine_type      = io::routing_engine<engine_policy, engine_transport, std::chrono::steady_clock, typename detail::profile_traits<Profile>::peer_storage,
                                                typename detail::profile_traits<Profile>::topic_storage, typename detail::profile_traits<Profile>::liveliness_storage,
                                                typename detail::profile_traits<Profile>::graph_change_log, typename detail::profile_traits<Profile>::peer_report_emitter,
                                                engine_forward_splice>;
    using engine_channel   = typename engine_type::channel_type;
    using transport_tuple  = std::tuple<Transports...>;

    // The self-route engages whenever the pack carries an intra_node_transport: the pure
    // single-transport node binds the loopback channel as channel_type (concrete, zero erasure,
    // object fast path); a composed intra-node+network node binds polymorphic_byte_channel, so the
    // self-channel is wrapped (erased, framed bytes lane only — no zero-copy self-lane there). Any
    // pack with no intra-node member leaves the self-route idle.
    static constexpr bool k_has_self_loopback = detail::pack_has_intra_node<policy_type, Transports...>;

    // Single-transport: the engine's channel IS the concrete loopback channel, attached directly.
    // Multi-transport: the engine's channel is the erased polymorphic_byte_channel, so the concrete
    // self-channel is reference-adapter wrapped to satisfy the registry's reference_wrapper.
    static constexpr bool k_self_route_erased = k_has_self_loopback && !std::is_same_v<engine_channel, io::process_loopback_channel<policy_type>>;

    // The fallback self-carrier: when no intra-node transport serves self but the pack has an shm
    // member, a same-node publish self-delivers over a node-local shm self-ring (the framed bytes
    // lane). Mutually exclusive with the intra-node self-route — at most one carrier per node.
    static constexpr bool k_has_shm_self_carrier = !k_has_self_loopback && detail::pack_has_shm_member<Transports...>;
    static_assert(!(k_has_self_loopback && k_has_shm_self_carrier), "a node installs at most one self-route carrier (intra-node OR shm self-ring), never both");
    static_assert(detail::shm_member_count<Transports...> <= 1, "a node composes at most one shared-memory member");

    // The node_id is taken verbatim: plexus compares the identity, never mints or interprets it.
    basic_node(executor_type executor, discovery::discovery &disc, const plexus::node_id &id, Transports &...transports, const node_options &opts)
            : m_id(id)
            , m_executor(executor)
            , m_disc(disc)
            , m_logger(resolve_logger(opts))
            , m_service_name(opts.name.empty() ? io::node_name_of(id) : opts.name)
            , m_max_message_bytes(opts.max_message_bytes)
            , m_wire_crypto_position(opts.wire.position)
            , m_ops()
            , m_close_notification()
            // The leaves are BORROWED three ways and never consumed (mux glue, resolve_hook's
            // by-ref capture, engine_leaf's re-read), so a future edit must NOT move into the mux
            // ctor — that would invalidate the subsequent engine_leaf / resolve_hook reads.
            , m_glue(transports..., io::transport_selector{}, detail::resolve_hook(transports...))
            , m_leaf(engine_leaf(transports...))
            , m_self_channel(executor, m_service_name)
            , m_engine(m_leaf, executor, make_fsm_cfg(id, opts), opts.handshake_timeout, opts.reconnect, opts.redial_seed, resolve_logger(opts), opts.dial_eagerly,
                       opts.max_message_bytes, opts.liveliness, opts.routes, opts.report)
    {
        // The routes are installed and the card advertised synchronously in the ctor turn, before
        // any session is built (the set-before-listen contract).
        m_engine.on_message_route([this](std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info) { dispatch_message(fqn, bytes, info); });
        m_engine.on_object_route([this](std::string_view fqn, const io::object_carrier &carrier) { detail::dispatch_object(*this, fqn, carrier); });
        m_engine.add_observer(m_peer_watch);
        detail::advertise_card(*this);
        m_disc.browse([this](const discovery::service_info &peer) { detail::note_from_card(*this, peer); });
        m_disc.on_withdrawn([this](const discovery::service_info &peer) { detail::forget_from_card(*this, peer); });
        detail::install_upgrade_coordinator(*this, opts.report.universe);
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

    // A dtor must not throw — the post is wrapped. A closed node posted its destroyed edge when it closed.
    ~basic_node()
    {
#if defined(__cpp_exceptions)
        try
        {
            if(!m_engine.closing())
                detail::post_destroyed_edge(*this);
        }
        catch(...)
        {
        }
#else
        if(!m_engine.closing())
            detail::post_destroyed_edge(*this);
#endif
        detail::withdraw_fallback_counting(*this);
    }

    // Withdraws the node from discovery and closes every session, channel, transport, timer and pending call it holds
    // without arming a redial. on_closed then fires once, posted, after the last of them has completed.
    void close()
    {
        detail::close_node(*this);
    }

    // A handler installed after the node settled is posted when installed; a delivered notification never repeats.
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_close_notification.install(std::move(cb));
        detail::notify_node_closed(*this);
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
            detail::advertise_card(*this);
        }
    }

    // A directly-dialed endpoint is not advertisable (no card, no listen key): forward to the engine
    // and nothing else. The peer's real id arrives in the handshake.
    void dial(const io::endpoint &ep)
    {
        m_engine.dial(ep);
    }

    // Withdraws a dial made with dial(): the endpoint is never redialed and its session is retired. dial() again
    // re-adds it.
    void hang_up(const io::endpoint &ep)
    {
        m_engine.hang_up(ep);
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
        return detail::sweep_participants(m_engine.known(), out);
    }

    // Executor-affine: call only on the owning executor. Non-blocking; sweeps engine state with no
    // lock and no allocation (the participants() shape) and discloses the posture through the logger
    // at least once so relay involvement is never silent (the default_discovery::self_check()
    // warn-once precedent). offering_relay is decided at COMPILE time off the emitter twin: a node not
    // spelled relay<> instantiates none of the offering-query branch, so no emitter-query symbol
    // reaches the leaf image.
    relay_posture self_check() const
    {
        const relay_posture posture = detail::compute_relay_posture(*this);
        detail::disclose_relay_posture_once(*this, posture);
        return posture;
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

    // Registers a graph-change observer over the engine's add_observer seam and returns a move-only
    // RAII handle whose dtor deregisters: the teardown ordering is off the consumer, closing the
    // re-entry footgun. A callback taking a graph::graph_change subscribes to the host {who, kind}
    // delta — present on the heap profile, structurally absent on bounded<> (its null log twin holds
    // nothing); a callback taking the coarse std::uint64_t generation dedupes on it and re-snapshots
    // participants()/topics() only when the generation advanced. The engine drains posted over the
    // borrowed executor (the class's LIFETIME contract), so the handle must be dropped before the
    // executor stops.
    template<typename Cb>
    graph_change_handle on_graph_change(Cb cb)
    {
        if constexpr(std::is_invocable_v<Cb &, const graph::graph_change &>)
            return graph_change_handle{m_engine, {}, std::move(cb)};
        else
            return graph_change_handle{m_engine, std::move(cb)};
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

    // How often a same-host lane was declined, demoted or lost, indexed by io::fallback_reason; a declined or lost
    // pair keeps the wire.
    io::fallback_counts same_host_fallbacks() const noexcept
    {
        return m_engine.same_host_fallbacks();
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
    recorder<engine_type, policy_type> make_recorder(io::recording::byte_sink &sink, recorder_options opts = {})
    {
        return recorder<engine_type, policy_type>{m_engine, m_executor, m_id, sink, std::move(opts), m_wire_crypto_position};
    }

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
        const bool first_for_fqn  = !m_subscriptions.any_for(fqn);
        const registration_id rid = m_subscriptions.add(subscription{std::string{fqn}, qos, type_id, std::string{type_name}, std::move(cb), std::move(obj)});
        if(first_for_fqn)
            self_attach(fqn, qos, type_id);
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
        detail::provision_same_host_ring(*this, fqn, subscriber_effective_bytes(qos), std::nullopt);
        m_engine.note_local_topic(fqn, type_name, type_id, graph::topic_role::subscriber);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::subscriber_registered, wire::fqn_topic_hash(fqn), type_id});
        for(const auto &peer : m_known_peers)
            m_engine.subscribe(peer, fqn, qos, io::locality::any, io::reliability_requirement::any, type_id, type_name);
        return rid;
    }

    void retire_subscriber_seam(registration_id rid)
    {
        if(const std::optional<std::string> fqn = m_subscriptions.remove(rid))
            retire_fqn_demand(*fqn);
    }

    // Reached only once the LAST subscriber on the fqn is gone, which is what makes dropping the
    // one (participant, topic, subscriber) edge here correct: N handles are one edge, not N.
    void retire_fqn_demand(std::string_view fqn)
    {
        self_detach(fqn);
        m_engine.forget_local_topic(fqn, graph::topic_role::subscriber);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::subscriber_retired, wire::fqn_topic_hash(fqn), std::nullopt});
        for(const auto &peer : m_known_peers)
            m_engine.unsubscribe(peer, fqn);
    }

    // The declaration persists for the node's life (stable identity for subscriber correlation);
    // the handle's lifetime is distinct, so retire_publisher_seam only posts the drop edge.
    void declare_publisher_seam(std::string_view fqn, const topic_qos &qos, bool emit_source_identity, std::optional<std::uint64_t> type_id = std::nullopt,
                                std::string_view type_name = {}, std::uint64_t schema_hint = 0, std::optional<io::same_host_geometry> geometry = std::nullopt,
                                std::optional<io::topic_capture_rule> capture = std::nullopt)
    {
        m_engine.messages().declare(fqn, qos, type_id, emit_source_identity, type_name, schema_hint);
        if(capture)
            m_engine.capture().set_topic(wire::fqn_topic_hash(fqn), *capture);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::publisher_declared, wire::fqn_topic_hash(fqn), type_id, type_name, schema_hint});
        const std::size_t effective_bytes = io::effective_max(qos, m_max_message_bytes);
        detail::provision_same_host_ring(*this, fqn, effective_bytes, geometry);
        m_engine.coordinator().set_topic_hint(fqn, qos.dispatch);
        detail::note_local_publisher(*this, fqn, type_name, type_id);
    }

    void retire_publisher_seam(std::string_view fqn)
    {
        detail::forget_local_publisher(*this, fqn);
        m_engine.post_endpoint(fqn, {io::endpoint_edge::publisher_dropped, wire::fqn_topic_hash(fqn), std::nullopt});
    }

    std::size_t subscriber_effective_bytes(const io::subscriber_qos &qos) const noexcept
    {
        return qos.requested_max_message_bytes != 0 ? static_cast<std::size_t>(qos.requested_max_message_bytes) : m_max_message_bytes;
    }

    void dispatch_message(std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info)
    {
        detail::subscription_table::dispatch_guard guard{m_subscriptions};
        for(auto &[rid, sub] : m_subscriptions.entries())
            if(sub.fqn == fqn && !m_subscriptions.pending_removed(rid))
                sub.cb(bytes, info);
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
            detail::dispatch_object(*this, fqn, carrier);
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
            detail::sync_shm_self_carrier(*this, fqn);
    }

    void self_detach(std::string_view fqn)
    {
        if constexpr(k_has_self_loopback)
            m_engine.messages().detach_local(fqn, self_route());
        else if constexpr(k_has_shm_self_carrier)
            detail::sync_shm_self_carrier(*this, fqn);
    }

    // Each verb is a captureless static lambda (a plain fn-ptr, zero alloc) recovering the node and
    // forwarding to the private *_seam, so the concrete Policy stays inside those bodies.
    io::endpoint_seam endpoint_seam_for() noexcept
    {
        io::endpoint_seam s{};
        s.ctx               = this;
        s.declare_publisher = [](void *ctx, std::string_view fqn, const topic_qos &qos, bool emit, std::optional<std::uint64_t> type_id, std::string_view type_name,
                                 std::uint64_t schema_hint, std::optional<io::same_host_geometry> geometry, std::optional<io::topic_capture_rule> capture)
        { static_cast<basic_node *>(ctx)->declare_publisher_seam(fqn, qos, emit, type_id, type_name, schema_hint, geometry, capture); };
        s.publish        = [](void *ctx, std::string_view fqn, std::span<const std::byte> bytes) { static_cast<basic_node *>(ctx)->m_engine.messages().publish(fqn, bytes); };
        s.publish_object = [](void *ctx, std::string_view fqn, const io::object_carrier &carrier, io::encode_thunk encode)
        { static_cast<basic_node *>(ctx)->m_engine.messages().publish_object(fqn, carrier, [&] { return io::invoke(encode); }); };
        s.register_subscriber = [](void *ctx, std::string_view fqn, const io::subscriber_qos &qos, io::bytes_cb cb, std::optional<std::uint64_t> type_id, std::string_view type_name,
                                   const void *native_key, io::object_dispatch dispatch, std::optional<io::topic_capture_rule> capture) -> registration_id
        { return static_cast<basic_node *>(ctx)->register_subscriber_seam(fqn, qos, std::move(cb), type_id, type_name, object_entry{native_key, std::move(dispatch)}, capture); };
        s.retire_subscriber = [](void *ctx, registration_id rid) { static_cast<basic_node *>(ctx)->retire_subscriber_seam(rid); };
        s.retire_publisher  = [](void *ctx, std::string_view fqn) { static_cast<basic_node *>(ctx)->retire_publisher_seam(fqn); };
        s.serve_procedure   = [](void *ctx, std::string_view fqn, io::handler_fn handler) { detail::serve_procedure_seam(*static_cast<basic_node *>(ctx), fqn, std::move(handler)); };
        s.retire_procedure  = [](void *ctx, std::string_view fqn) { detail::retire_procedure_seam(*static_cast<basic_node *>(ctx), fqn); };
        s.call              = [](void *ctx, std::string_view fqn, std::span<const std::byte> param, io::on_reply_fn on_reply, std::optional<std::chrono::nanoseconds> deadline)
        { detail::call_seam(*static_cast<basic_node *>(ctx), fqn, param, std::move(on_reply), deadline); };
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

    plexus::node_id m_id;
    executor_type m_executor;
    discovery::discovery &m_disc;
    // Declared immediately ABOVE m_logger so it is fully constructed before resolve_logger binds
    // either reference in the init list when no logger is supplied.
    log::null_logger m_default_logger;
    log::logger &m_logger;
    mutable bool m_relay_posture_warned{false};
    std::string m_service_name;
    std::string m_host;
    std::vector<discovery::listening_transport> m_listens;
    std::size_t m_max_message_bytes;
    wire_crypto_position m_wire_crypto_position;
    io::detail::pending_ops m_ops;
    io::detail::close_notification m_close_notification;

    // The non-movable mux is never a prvalue, so the empty single-transport glue keeps its
    // zero-size overlap under [[no_unique_address]].
    [[no_unique_address]] detail::node_mux_glue<Transports...> m_glue;

    engine_transport &m_leaf;

    // Declared BEFORE the engine (which captures &m_peer_watch and a `this`-bound route) so the
    // engine is destroyed FIRST, leaving no dangling observer/route reference during teardown.
    peer_watch m_peer_watch{*this};
    std::vector<plexus::node_id> m_known_peers;
    detail::subscription_table m_subscriptions;

    std::vector<std::string> m_served_fqns;
    std::size_t m_object_dispatch_mismatch{};

    // The node-owned self-route, attached to its OWN forwarder so a publish reaches a same-node
    // subscriber. Declared BEFORE m_engine so it outlives the registry references that hold it.
    // It is only wired when the pack carries an intra-node transport; on any other node it is idle.
    io::process_loopback_channel<policy_type> m_self_channel;

    // On a multi-transport node the registry holds reference_wrapper<polymorphic_byte_channel>, so the
    // concrete self-channel is erased through a non-owning reference adapter (the node keeps owning
    // m_self_channel). [[no_unique_address]] on the single-transport monostate keeps it zero-size.
    using self_route_holder = std::conditional_t<k_self_route_erased, io::polymorphic_byte_channel, detail::no_self_route_wrapper>;
    [[no_unique_address]] self_route_holder m_self_route{detail::make_self_route<self_route_holder>(m_self_channel)};

    // Live local-publisher count per fqn. It settles two questions: the node's own publisher edge
    // retires on the last handle rather than the first, and the shm self-ring mints only when a
    // same-node publisher and subscriber coexist (so a node that only talks remote mints no region).
    std::unordered_map<std::string, std::size_t> m_local_publishers;

    // The read half of each topic's shm self-carrier (the fallback self-route when no intra-node transport serves self);
    // its write half is the coordinator's lane for the topic.
    std::unordered_map<std::string, std::shared_ptr<void>> m_self_carriers;

    engine_type m_engine;

    template<typename N>
    friend void detail::note_known_peer(N &, const plexus::node_id &);
    template<typename N>
    friend void detail::fan_demands_to(N &, const plexus::node_id &);
    template<typename N>
    friend void detail::advertise_card(N &);
    template<typename N>
    friend void detail::note_from_card(N &, const discovery::service_info &);
    template<typename N>
    friend void detail::forget_from_card(N &, const discovery::service_info &);
    template<typename N, typename R>
    friend void detail::call_seam(N &, std::string_view, std::span<const std::byte>, R, std::optional<std::chrono::nanoseconds>);
    template<typename N>
    friend void detail::serve_procedure_seam(N &, std::string_view, io::handler_fn);
    template<typename N>
    friend void detail::retire_procedure_seam(N &, std::string_view);
    template<typename N>
    friend void detail::note_local_publisher(N &, std::string_view, std::string_view, std::optional<std::uint64_t>);
    template<typename N>
    friend void detail::forget_local_publisher(N &, std::string_view);
    template<typename N, typename F>
    friend void detail::for_each_shm_member(N &, F &&);
    template<typename N>
    friend void detail::install_upgrade_coordinator(N &, std::uint32_t);
    template<typename N>
    friend void detail::sync_shm_self_carrier(N &, std::string_view);
    template<typename N>
    friend void detail::install_shm_self_carrier(N &, const std::string &);
    template<typename N>
    friend void detail::teardown_shm_self_carrier(N &, const std::string &);
    template<typename N>
    friend void detail::self_carrier_bytes(N &, std::string_view, std::span<const std::byte>);
    template<typename N>
    friend void detail::dispatch_object(N &, std::string_view, const io::object_carrier &);
    template<typename N>
    friend void detail::post_destroyed_edge(N &);
    template<typename N>
    friend void detail::close_node(N &);
    template<typename N>
    friend void detail::close_self_route(N &);
    template<typename N>
    friend void detail::notify_node_closed(N &);
    template<typename N>
    friend std::size_t detail::count_routes_via_relays(const N &);
    template<typename N>
    friend relay_posture detail::compute_relay_posture(const N &);
    template<typename N>
    friend void detail::disclose_relay_posture_once(const N &, const relay_posture &);
};

// Transitional: every existing plexus::node<...> spelling migrates onto basic_node through this
// alias. It retires once call sites respell, freeing the plexus::node name for the eventual
// non-templated native default.
template<typename Policy, typename... Transports>
using node = basic_node<Policy, Transports...>;

}

#endif
