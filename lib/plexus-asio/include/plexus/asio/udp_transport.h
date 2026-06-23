#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_TRANSPORT_H

#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_inbound_demux.h"
#include "plexus/asio/detail/udp_transport_setup.h"
#include "plexus/asio/detail/asio_udp_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/datagram/mtu_budget.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/datagram/detail/udp_handshake_arq.h"
#include "plexus/datagram/detail/udp_handshake_frame.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <random>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::asio {

// over-limit: one cohesive UDP transport_backend; the listen/dial/close verbs + the config the
// ctor binds all drive the shared m_server/m_demux/m_dials state (the transport_backend concept
// proof at file bottom binds the surface), so splitting the public face scatters that shared
// state — the accept-loop/dial-resolution/handshake glue is extracted to
// detail/udp_transport_setup.h.
//
// The connectionless UDP transport_backend: owns the ONE bound udp_server, the inbound demux
// (sender host:port -> channel), the per-peer handshake-ARQ dial table, and every minted
// udp_channel. It mints logical channels over the shared socket (NO acceptor). listen(ep) binds
// the server + arms the recv loop (a MISS from a never-seen source synthesizes an accept only
// from a handshake request — the source endpoint is NOT trusted as identity, T-15-03). dial(ep)
// mints a channel + drives the handshake ARQ, firing on_dialed ON RESOLUTION. close() cancels
// every pending ARQ timer then drops the channels and the socket.
class udp_transport
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;
    using arq_type      = datagram::detail::udp_handshake_arq<udp_policy>;
    using hs_type       = datagram::detail::udp_hs_type;

    // global_default is the node-level per-MESSAGE size ceiling and reassembly_budget the
    // always-on aggregate reassembly-memory cap — the two operator-facing message-size
    // node options (required-WITH-default, bound to the shipped named constants). Stamped
    // into every minted channel; a per-topic override resolves through io::effective_max.
    // max_payload here remains the per-FRAGMENT MTU budget (a separate axis).
    // backpressure_bytes is the per-channel bounded send queue for fragments awaiting ARQ
    // window room (required-WITH-default). It bounds ADDITIONAL backlog only — the channel
    // floors the live cap at the per-message ceiling, so a single within-ceiling message's
    // fragment backlog is always admissible regardless of this knob (the ceiling, not this
    // cap, is the message-size authority); raise it only to allow extra backlog past one message.
    explicit udp_transport(::asio::io_context &io,
                           std::size_t         max_payload = udp_channel::default_max_payload,
                           arq_type::schedule  hs_ladder   = arq_type::default_ladder,
                           datagram::detail::udp_arq_config arq_cfg    = {},
                           io::congestion                   congestion = io::congestion::block,
                           std::size_t max_peers = detail::udp_inbound_demux::default_max_peers,
                           std::size_t so_sndbuf = udp_server::default_so_sndbuf,
                           std::size_t so_rcvbuf = udp_server::default_so_rcvbuf,
                           std::size_t send_queue_bytes   = udp_server::default_send_queue_bytes,
                           std::size_t global_default     = io::global_default_max_message_bytes,
                           std::size_t reassembly_budget  = io::reassembly_memory_budget,
                           std::size_t backpressure_bytes = udp_channel::default_backpressure_bytes,
                           std::chrono::milliseconds reassembly_timeout =
                                   udp_channel::reassembler_type::config{}.per_message_timeout)
            : m_io(io)
            , m_server(io, congestion, send_queue_bytes, so_sndbuf, so_rcvbuf)
            , m_max_peers(max_peers)
            , m_demux(max_peers)
            , m_max_payload(max_payload)
            , m_global_default(global_default)
            , m_reassembly_budget(reassembly_budget)
            , m_backpressure_bytes(backpressure_bytes)
            , m_reassembly_timeout(reassembly_timeout)
            , m_hs_ladder(hs_ladder)
            , m_arq_cfg(arq_cfg)
            , m_congestion(congestion)
            , m_dials(make_defer_destroy())
    {
        m_server.on_datagram([this](const endpoint_type &from, std::span<const std::byte> bytes)
                             { detail::on_datagram(*this, from, bytes); });
        m_server.on_error(
                [this](io::io_error e)
                {
                    if(m_on_error)
                        m_on_error(e);
                });
    }

    udp_transport(const udp_transport &)            = delete;
    udp_transport &operator=(const udp_transport &) = delete;

    ~udp_transport() { close(); }

    // The concrete channel this member's completions deliver + its routing identity: the
    // schemes it serves and the locality tier. The ONE datagram member serves BOTH "udp"
    // (best_effort) and "udpr" (reliable-datagram, the ARQ engaged) — the channel's mode
    // differs by scheme, the member is the same; a reliable_datagram demand is never served
    // over bare UDP. A generic multiplexer reads these at compile time to route over a pack.
    using channel_type = udp_channel;
    static constexpr std::array<std::string_view, 2> mux_schemes{"udp", "udpr"};
    static constexpr io::transport_kind              mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>,
                                                           const io::endpoint &)>
                           cb)
    {
        m_on_dialed = std::move(cb);
    }
    void
    on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }

    // The drop-observability seam (null by default — zero cost when unobserved). The owner
    // installs the engine's posted drop_sink; an inbound flood refused by the per-peer
    // demux cap emits demux_refused here. The sink POSTS, so the spoof-flood refusal site
    // never fires the observer synchronously (the cap exists to bound exactly that flood).
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void listen(const io::endpoint &ep)
    {
        std::error_code pec;
        auto            bind_ep = detail::parse_udp(ep.address, pec);
        if(pec)
            return detail::report_error(*this, detail::map_error(pec));
        m_server.start(bind_ep);
    }

    // dial parses, mints a channel + a handshake ARQ, and fires on_dialed on the ARQ
    // resolving (the paired hs_response arriving) — NOT immediately. ep rides through
    // the ARQ closures so the engine correlates the completion by endpoint. The scheme
    // selects the channel mode: "udpr" -> the reliable-datagram ARQ class, anything else
    // -> best_effort. The mode is declared in the handshake so the acceptor is symmetric.
    // NOLINTNEXTLINE(readability-function-size)
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto            dest = detail::parse_udp(ep.address, pec);
        if(pec)
            return detail::report_dial_fail(*this, ep, detail::map_error(pec));

        detail::ensure_bound(*this, dest.protocol()); // a dial-only transport still needs a socket

        const auto          mode = mode_of_scheme(ep.scheme);
        const std::uint16_t isn  = next_isn(); // the dialer's per-session ISN, in the request
        auto                ch   = std::make_unique<udp_channel>(
                m_io, m_server, dest, m_max_payload, m_arq_cfg, m_congestion, m_backpressure_bytes,
                mode, isn, m_global_default, m_reassembly_budget, m_reassembly_timeout);
        auto *raw = ch.get();
        m_demux.insert(dest, raw);
        detail::wire_teardown(*this, *raw, dest);

        auto  arq     = std::make_unique<arq_type>(m_io, m_hs_ladder);
        auto *raw_arq = arq.get();
        raw_arq->on_transmit([this, dest, mode, isn]
                             { detail::send_handshake(*this, dest, hs_type::request, mode, isn); });
        raw_arq->on_established([this, ep, raw] { detail::resolve_dial(*this, ep, raw); });
        raw_arq->on_timeout([this, ep, raw] { detail::fail_dial(*this, ep, raw); });

        m_dials.insert(raw, std::move(ch), std::move(arq));
        raw_arq->start();
    }

    void close()
    {
        // Drop both registry tables, then the demux and the socket. clear() destroys each
        // held channel AND its per-entry handshake ARQ synchronously from this own-close
        // path (never from inside a channel's member call). Destroying an ARQ cancels its
        // pending retransmit timer (the timer's queued completion guards on the aborted
        // error before touching the freed ARQ), so the "cancel every pending ARQ timer
        // then drop" teardown semantics hold.
        m_dials.clear();
        m_demux = detail::udp_inbound_demux{m_max_peers};
        m_server.close();
    }

    [[nodiscard]] std::uint16_t port() const { return m_server.port(); }

private:
    using dial_registry = io::pending_dial_registry<udp_channel, std::unique_ptr<arq_type>>;

    // The deferred-destroy sink the registry routes a failed channel through: a fail
    // edge may fire from inside the channel's own member stack, so destroying it there
    // frees it mid-unwind. Posting it to a continuation that owns it until it runs defers
    // the destruction off the current stack (the UDP fail is a clean timer callback, so
    // this defer is harmless here and strictly safe).
    dial_registry::defer_destroy make_defer_destroy()
    {
        return [this](std::unique_ptr<udp_channel> ch)
        { ::asio::post(m_io, [ch = std::move(ch)]() mutable { ch.reset(); }); };
    }

    // The scheme -> channel-mode classifier: "udpr" requests the reliable-datagram ARQ
    // class; every other scheme (including bare "udp") is best_effort. The mirror of the
    // mux selector's reliability_of_scheme, applied at the datagram member's dial face.
    [[nodiscard]] static datagram::detail::udp_channel_mode
    mode_of_scheme(const std::string &scheme) noexcept
    {
        return scheme == "udpr" ? datagram::detail::udp_channel_mode::reliable_datagram
                                : datagram::detail::udp_channel_mode::best_effort;
    }

    // Draw a per-session ISN (RFC 6528 lineage) DIRECTLY from std::random_device, the OS
    // CSPRNG (getrandom / BCryptGenRandom / arc4random on the supported platforms). A setup-
    // time event (one draw per dial/accept), NOT a per-packet RNG. The former shared
    // std::mt19937 stream was reconstructible from ISNs echoed in cleartext handshakes,
    // letting an attacker predict CONCURRENT sessions' ISNs; a per-session OS-entropy draw
    // stores no reconstructible stream state, so observing one session's ISN reveals nothing
    // about another's. The range omits 0 so a negotiated ISN is always distinguishable from
    // the legacy back-compat default (an absent ISN field decodes 0).
    std::uint16_t next_isn()
    {
        // random_device::result_type is at least 32 bits and non-deterministic by contract;
        // fold it into [1, 0xFFFF]. The modulo bias across a 16-bit window of a >=32-bit
        // draw is negligible for an unpredictability (not uniformity) requirement.
        return static_cast<std::uint16_t>(m_isn_rng() % 0xFFFFu) + 1u;
    }

    // The accept-loop / dial-resolution / handshake glue is relocated to
    // detail/udp_transport_setup.h (relocation by friendship): each helper reaches the
    // server/demux/dial-table members below through the transport reference.
    template<typename U>
    friend void detail::report_dial_fail(U &, const io::endpoint &, io::io_error);
    template<typename U>
    friend void detail::report_error(U &, io::io_error);
    template<typename U>
    friend void detail::ensure_bound(U &, const ::asio::ip::udp &);
    template<typename U>
    friend void detail::send_handshake(U &, const typename U::endpoint_type &, typename U::hs_type,
                                       datagram::detail::udp_channel_mode, std::uint16_t);
    template<typename U>
    friend void detail::wire_teardown(U &, udp_channel &, const typename U::endpoint_type &);
    template<typename U>
    friend void detail::resolve_paired(U &, udp_channel *);
    template<typename U>
    friend void detail::resolve_dial(U &, const io::endpoint &, udp_channel *);
    template<typename U>
    friend void detail::fail_dial(U &, const io::endpoint &, udp_channel *);
    template<typename U>
    friend void detail::accept_new_peer(U &, const typename U::endpoint_type &,
                                        std::span<const std::byte>);
    template<typename U>
    friend void detail::route_to_peer(U &, const typename U::endpoint_type &, udp_channel *,
                                      std::span<const std::byte>);
    template<typename U>
    friend void detail::on_datagram(U &, const typename U::endpoint_type &,
                                    std::span<const std::byte>);

    ::asio::io_context       &m_io;
    udp_server                m_server;
    std::size_t               m_max_peers;
    detail::udp_inbound_demux m_demux;
    std::size_t               m_max_payload;    // per-FRAGMENT MTU budget (NOT the message ceiling)
    std::size_t               m_global_default; // node-level per-MESSAGE size ceiling
    std::size_t               m_reassembly_budget; // aggregate reassembly-memory cap (always-on)
    std::size_t m_backpressure_bytes; // per-channel bounded send queue for windowed fragments
    std::chrono::milliseconds        m_reassembly_timeout; // per-message reassembly reclaim window
    arq_type::schedule               m_hs_ladder;
    datagram::detail::udp_arq_config m_arq_cfg;
    io::congestion                   m_congestion;
    std::random_device
            m_isn_rng; // OS CSPRNG; one draw per dial/accept (no hot-path RNG, no stream state)
    std::vector<std::byte> m_hs_scratch;
    dial_registry          m_dials; // the half-open dial table + the accepted table
    plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>, const io::endpoint &)>
                                                                                 m_on_dialed;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)>     m_on_drop;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)>                       m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::udp_transport, plexus::asio::udp_policy>,
              "udp_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
