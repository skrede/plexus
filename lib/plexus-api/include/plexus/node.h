#ifndef HPP_GUARD_PLEXUS_NODE_H
#define HPP_GUARD_PLEXUS_NODE_H

#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/node_name.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/muxify.h"
#include "plexus/node_id.h"
#include "plexus/policy.h"

#include <tuple>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <charconv>
#include <optional>
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

// The consumable public surface: a node composes a routing_engine over an injected
// substrate. Policy is explicit (the plexus convention); the trailing transport pack
// is deduced. A single transport binds Policy directly; two or more bind muxify<Policy>
// over a node-owned multiplexing_transport (D-07). Omitting any substrate element —
// executor, discovery, the transports, or the options — is a COMPILE ERROR (the
// injected-only gate, D-06): there is no owning convenience overload.
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
public:
    using executor_type = typename Policy::executor_type;
    using engine_policy = detail::node_engine_policy<Policy, Transports...>;
    using engine_transport = detail::node_engine_transport<Transports...>;
    using engine_type =
        io::routing_engine<engine_policy, engine_transport>;

    // The node_id is taken VERBATIM (API-06): plexus compares the identity, never
    // mints or interprets it.
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
        // D-02: advertise the (initially port-less) contact card at construction so a
        // dial-only node is discoverable from birth, and browse to awareness. These run
        // synchronously in the ctor turn (not posted) — they install state before any
        // session exists, the set-before-listen contract.
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
    // the live contact card and re-advertise (the D-02 live-update path). PRECONDITION:
    // an advertised listen requires an EXPLICIT port in ep.address ("host:port") — a
    // port-0 auto-assign cannot be advertised this phase, because the transport_backend
    // concept exposes no bound-port accessor (a concept extension is the seeded path). A
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

    // The node's own identity (API-06): verbatim from the ctor, or the name-hash
    // derivation. The authoritative self identity the engine handshakes with.
    const plexus::node_id &id() const noexcept { return m_id; }

    // Escape hatches (API-05): the live engine objects, for advanced peer-level work
    // the topic-level public verbs deliberately hide.
    engine_type &router() noexcept { return m_engine; }
    const engine_type &router() const noexcept { return m_engine; }
    auto &message_forwarder() noexcept { return m_engine.messages(); }
    executor_type executor() const noexcept { return m_executor; }

private:
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

    // Parse a browsed card into an awareness entry (D-02). Every step is reject-on-
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
            m_engine.note_peer(*peer_id, *ep);
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
    engine_type m_engine;
};

}

#endif
