#ifndef HPP_GUARD_PLEXUS_NODE_H
#define HPP_GUARD_PLEXUS_NODE_H

#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/node_name.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/peer_observer.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/muxify.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

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

template <typename Policy, typename Codec = void>
    requires plexus::Policy<Policy>
class publisher;

template <typename Policy, typename Codec = void>
    requires plexus::Policy<Policy>
class subscriber;

template <typename Policy, typename Sig = void, typename CReq = void, typename CRes = void>
    requires plexus::Policy<Policy>
class caller;

template <typename Policy, typename Sig = void, typename CReq = void, typename CRes = void>
    requires plexus::Policy<Policy>
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
    template <typename P, typename C> requires plexus::Policy<P> friend class publisher;
    template <typename P, typename C> requires plexus::Policy<P> friend class subscriber;
    template <typename P, typename S, typename Cq, typename Cs>
        requires plexus::Policy<P> friend class caller;
    template <typename P, typename S, typename Cq, typename Cs>
        requires plexus::Policy<P> friend class procedure;

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
        , m_service_name(opts.name.empty() ? io::node_name_of(id) : opts.name)
        , m_glue(make_glue(transports...))
        , m_engine(engine_leaf(transports...), executor, make_fsm_cfg(id, opts),
                   opts.handshake_timeout, opts.reconnect, opts.redial_seed,
                   opts.dial_eagerly, resolve_logger(opts))
    {
        // The fqn-to-callback demux: install the node-shared receive route ONCE, before
        // any session is built (the route's set-before-listen contract). Every delivered
        // data frame fans to every callback locally registered for its fqn.
        m_engine.on_message_route(
            [this](std::string_view fqn, std::span<const std::byte> bytes, const io::message_info &info)
            { dispatch_message(fqn, bytes, info); });
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
        if(const auto port = port_of(ep.address))
        {
            if(m_host.empty())
                m_host = host_of(ep.address);
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

private:
    // ---- Endpoint infrastructure (the topic->peer translation) ----------------------
    //
    // A locally registered subscriber: its fqn, the requested qos, and the callback the
    // demux fans a delivered frame to. Keyed by a monotonically minted registration id
    // so two subscribers on one fqn coexist and retire independently (the id is never
    // reused, so a stale retire never collides with a later registration).
    struct subscription
    {
        std::string        fqn;
        io::subscriber_qos qos;
        // The subscriber-declared type identity (std::nullopt = undeclared), stored so a
        // late-discovered peer gets the typed demand fanned with the gate intact.
        std::optional<std::uint64_t> type_id;
        plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb;
    };

    // The standing-demand table doubling as the demux map. register_subscriber_seam
    // fans engine.subscribe to every known peer (now) and the re-fan reaches each peer
    // discovered/ready later; dispatch_message walks it on the receive path.
    using registration_id = std::uint64_t;

    // The private observer the node registers on its own engine: a peer-ready edge
    // re-fans every standing demand toward that peer (idempotent — remember_demand
    // dedups per (peer, fqn)). The fan-out runs POSTED on the borrowed executor, so this
    // bookkeeping needs no locking. node_id is recovered from the node_name key the edge
    // carries; an unparsable name is skipped (it never matched a known peer anyway).
    struct peer_watch : io::peer_observer
    {
        node &owner;
        explicit peer_watch(node &n) : owner(n) {}
        void on_peer_ready(const plexus::node_id &id, std::string_view, io::peer_kind) override
        {
            owner.note_known_peer(id);
            owner.fan_demands_to(id);
        }
    };

    // Register a standing subscriber: mint its id, store it, and fan its demand to every
    // currently known peer. Returns the id the retire seam keys on.
    registration_id register_subscriber_seam(
        std::string_view fqn, const io::subscriber_qos &qos,
        plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb,
        std::optional<std::uint64_t> type_id = std::nullopt)
    {
        const registration_id rid = m_next_registration++;
        m_subscriptions.push_back({rid, subscription{std::string{fqn}, qos, type_id, std::move(cb)}});
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
                                std::optional<std::uint64_t> type_id = std::nullopt)
    {
        m_engine.messages().declare(fqn, qos, type_id, emit_source_identity);
    }

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
    void serve_procedure_seam(std::string_view fqn,
                              typename io::procedure_forwarder<engine_policy>::handler_fn handler)
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

    bool any_subscriber_for(std::string_view fqn) const
    {
        return std::any_of(m_subscriptions.begin(), m_subscriptions.end(),
                           [&](const auto &e) { return e.second.fqn == fqn; });
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

    // The single-transport node has no glue (an empty member); the multi-transport
    // node constructs the multiplexing_transport from the borrowed leaves.
    static detail::node_mux_glue<Transports...> make_glue(Transports &...transports)
    {
        if constexpr(sizeof...(Transports) == 1)
            return detail::no_mux_glue{};
        else
            return io::multiplexing_transport<Transports...>{transports...};
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
        const auto peer_id = card_node_id(peer.metadata);
        if(!peer_id || *peer_id == m_id)
            return;
        const std::string host = host_of(peer.endpoint.address);
        if(host.empty())
            return;
        if(const auto ep = first_port_endpoint(peer.metadata, host))
        {
            m_engine.note_peer(*peer_id, *ep);
            note_known_peer(*peer_id);
            fan_demands_to(*peer_id);
        }
    }

    static std::optional<plexus::node_id>
    card_node_id(const std::vector<std::pair<std::string, std::string>> &card)
    {
        for(const auto &[k, v] : card)
            if(k == discovery::k_card_node_id_key)
                return discovery::detail::hex_decode(v);
        return std::nullopt;
    }

    // The host portion of a "host:port" address, with any trailing ":port" stripped.
    // An IPv6-style address (multiple colons) is left verbatim — only a single
    // trailing host:port pair is split.
    static std::string host_of(const std::string &address)
    {
        const auto colon = address.rfind(':');
        if(colon == std::string::npos)
            return address;
        return address.substr(0, colon);
    }

    // The first "plexus/<scheme>/port" key in card order, resolved to {scheme, host:port}.
    static std::optional<io::endpoint>
    first_port_endpoint(const std::vector<std::pair<std::string, std::string>> &card,
                        const std::string &host)
    {
        constexpr std::string_view k_prefix = "plexus/";
        constexpr std::string_view k_suffix = "/port";
        for(const auto &[k, v] : card)
        {
            std::string_view key{k};
            if(key.size() <= k_prefix.size() + k_suffix.size())
                continue;
            if(key.substr(0, k_prefix.size()) != k_prefix)
                continue;
            if(key.substr(key.size() - k_suffix.size()) != k_suffix)
                continue;
            const std::string_view scheme =
                key.substr(k_prefix.size(), key.size() - k_prefix.size() - k_suffix.size());
            if(const auto port = port_of_value(v))
                return io::endpoint{std::string{scheme}, host + ":" + std::to_string(*port)};
        }
        return std::nullopt;
    }

    // Parse the explicit port out of a "host:port" listen address (guarded — a missing
    // or non-numeric port yields absence, the auto-assign-not-advertisable precondition).
    static std::optional<std::uint16_t> port_of(const std::string &address)
    {
        const auto colon = address.rfind(':');
        if(colon == std::string::npos)
            return std::nullopt;
        return port_of_value(address.substr(colon + 1));
    }

    static std::optional<std::uint16_t> port_of_value(std::string_view v)
    {
        std::uint16_t port{};
        const char *first = v.data();
        const char *last = v.data() + v.size();
        const auto res = std::from_chars(first, last, port);
        if(res.ec != std::errc{} || res.ptr != last)
            return std::nullopt;
        return port;
    }

    plexus::node_id m_id;
    executor_type m_executor;
    discovery::discovery &m_disc;
    std::string m_service_name;
    std::string m_host;
    std::vector<discovery::listening_transport> m_listens;
    [[no_unique_address]] detail::node_mux_glue<Transports...> m_glue;

    // The endpoint state is declared BEFORE the engine so the engine — which captures
    // &m_peer_watch through add_observer and a `this`-bound route — is destroyed FIRST,
    // leaving no dangling observer/route reference during teardown.
    peer_watch m_peer_watch{*this};
    std::vector<plexus::node_id> m_known_peers;
    std::vector<std::pair<registration_id, subscription>> m_subscriptions;
    std::vector<std::string> m_served_fqns;
    registration_id m_next_registration{1};

    engine_type m_engine;
};

}

#endif
