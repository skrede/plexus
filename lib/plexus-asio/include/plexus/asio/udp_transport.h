#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_TRANSPORT_H

#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_inbound_demux.h"
#include "plexus/asio/detail/asio_udp_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/mtu_budget.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/io/detail/udp_handshake_arq.h"
#include "plexus/io/detail/udp_handshake_frame.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
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

// The connectionless UDP transport_backend: owns the ONE bound udp_server, the
// inbound demux (sender host:port -> channel), the per-peer handshake-ARQ dial table,
// and every minted udp_channel. It mints logical channels over the shared socket —
// there is NO acceptor and NO per-peer accept socket.
//
//   listen(ep): bind the shared server, arm the single recv loop. Each datagram is
//     demuxed by sender: a HIT routes to that channel; a MISS from a never-seen source
//     synthesizes an "accept" — a handshake request mints a channel + fires
//     on_accepted (and replies so the dialer's ARQ resolves), data alone is dropped
//     until a handshake establishes the peer (the source endpoint is NOT trusted as
//     identity — T-15-03).
//   dial(ep): parse host:port (fail-closed -> on_dial_failed); mint a channel bound to
//     the dest; drive the handshake ARQ (fixed 250/500/1000ms x3) and fire on_dialed
//     ON RESOLUTION (a UDP "connect" is local — the handshake proves reachability).
//     The endpoint rides the closure so the engine correlates by endpoint.
//   close(): cancel every pending ARQ timer, then drop the channels and the socket.
//
// The handshake exchange is a 1-byte control frame under the reliable_arq envelope
// kind: hs_request (the dialer's retransmitted probe) and hs_response (the acceptor's
// reply). It is the SAME kind discriminator a future DTLS path keys on to bypass the
// dedup+ARQ engine — design-in only.
class udp_transport
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;
    using arq_type = io::detail::udp_handshake_arq<udp_policy>;
    using hs_type = io::detail::udp_hs_type;

    explicit udp_transport(::asio::io_context &io, std::size_t max_payload = udp_channel::default_max_payload,
                           arq_type::schedule hs_ladder = arq_type::default_ladder,
                           io::detail::udp_arq_config arq_cfg = {},
                           io::congestion congestion = io::congestion::block)
        : m_io(io)
        , m_server(io, congestion)
        , m_max_payload(max_payload)
        , m_hs_ladder(hs_ladder)
        , m_arq_cfg(arq_cfg)
        , m_congestion(congestion)
        , m_isn_rng(std::random_device{}())
        , m_dials(make_defer_destroy())
    {
        m_server.on_datagram([this](const endpoint_type &from, std::span<const std::byte> bytes) { on_datagram(from, bytes); });
        m_server.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
    }

    udp_transport(const udp_transport &) = delete;
    udp_transport &operator=(const udp_transport &) = delete;

    ~udp_transport() { close(); }

    // The concrete channel this member's completions deliver + its routing identity: the
    // schemes it serves and the locality tier. The ONE datagram member serves BOTH "udp"
    // (best_effort) and "udpr" (reliable-datagram, the ARQ engaged) — the channel's mode
    // differs by scheme, the member is the same; a reliable_datagram demand is never served
    // over bare UDP. A generic multiplexer reads these at compile time to route over a pack.
    using channel_type = udp_channel;
    static constexpr std::array<std::string_view, 2> mux_schemes{"udp", "udpr"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep)
    {
        std::error_code pec;
        auto bind_ep = detail::parse_udp(ep.address, pec);
        if(pec)
            return report_error(detail::map_error(pec));
        m_server.start(bind_ep);
    }

    // dial parses, mints a channel + a handshake ARQ, and fires on_dialed on the ARQ
    // resolving (the paired hs_response arriving) — NOT immediately. ep rides through
    // the ARQ closures so the engine correlates the completion by endpoint. The scheme
    // selects the channel mode: "udpr" -> the reliable-datagram ARQ class, anything else
    // -> best_effort. The mode is declared in the handshake so the acceptor is symmetric.
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto dest = detail::parse_udp(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, detail::map_error(pec));

        ensure_bound(dest.protocol());      // a dial-only transport still needs a bound socket to send/recv

        const auto mode = mode_of_scheme(ep.scheme);
        const std::uint16_t isn = next_isn();   // the dialer's per-session ISN, advertised in the request
        auto ch = std::make_unique<udp_channel>(m_io, m_server, dest, m_max_payload, m_arq_cfg,
                                                m_congestion, udp_channel::default_backpressure_bytes, mode, isn);
        auto *raw = ch.get();
        m_demux.insert(dest, raw);
        wire_teardown(*raw, dest);

        auto arq = std::make_unique<arq_type>(m_io, m_hs_ladder);
        auto *raw_arq = arq.get();
        raw_arq->on_transmit([this, dest, mode, isn] { send_handshake(dest, hs_type::request, mode, isn); });
        raw_arq->on_established([this, ep, raw] { resolve_dial(ep, raw); });
        raw_arq->on_timeout([this, ep, raw] { fail_dial(ep, raw); });

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
        m_demux = detail::udp_inbound_demux{};
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
        {
            ::asio::post(m_io, [ch = std::move(ch)]() mutable { ch.reset(); });
        };
    }

    // Bind the shared socket to an ephemeral local endpoint if listen() has not
    // already bound it: a transport that only dials (never listens) still needs a
    // bound source port to send from and to receive the handshake replies on.
    void ensure_bound(const ::asio::ip::udp &proto)
    {
        if(!m_server.is_open())
            m_server.start(endpoint_type(proto, 0));
    }

    void on_datagram(const endpoint_type &from, std::span<const std::byte> bytes)
    {
        if(auto *ch = m_demux.lookup(from))
            return route_to_peer(from, ch, bytes);
        accept_new_peer(from, bytes);
    }

    // A known peer: a handshake control frame drives the ARQ / replies; anything else
    // is data the channel deduplicates and posts.
    void route_to_peer(const endpoint_type &from, udp_channel *ch, std::span<const std::byte> bytes)
    {
        if(auto hs = io::detail::decode_handshake(bytes))
        {
            if(hs->type == hs_type::response)
                resolve_paired(ch);
            else
                send_handshake(from, hs_type::response, hs->mode, hs->initial_seq);   // re-ack a retransmit, echo mode+ISN
            return;
        }
        ch->deliver_inbound(bytes);
    }

    // A never-seen source: ONLY a handshake request synthesizes an accept (the source
    // endpoint is not trusted as identity — bare data from an unknown source is
    // dropped). The demux cap bounds the spoof-flood (T-15-03). The dialer-declared mode
    // mints a symmetric channel (best_effort or the reliable-datagram ARQ class) and is
    // echoed in the response so the route flip is symmetric end-to-end.
    void accept_new_peer(const endpoint_type &from, std::span<const std::byte> bytes)
    {
        auto hs = io::detail::decode_handshake(bytes);
        if(!hs || hs->type != hs_type::request)
            return;
        // Bind the dialer's advertised ISN: the acceptor's receiver (reorder buffer) must
        // expect the peer's sender ISN, and the response ECHOES it (symmetric, like mode) so
        // both ends start the cumulative-ack edge from the same negotiated sequence.
        auto ch = std::make_unique<udp_channel>(m_io, m_server, from, m_max_payload, m_arq_cfg,
                                                m_congestion, udp_channel::default_backpressure_bytes,
                                                hs->mode, hs->initial_seq);
        auto *raw = ch.get();
        if(!m_demux.insert(from, raw))
            return;                                        // peer cap reached: drop the flood
        wire_teardown(*raw, from);
        m_dials.insert_accepted(raw, std::move(ch));
        send_handshake(from, hs_type::response, hs->mode, hs->initial_seq); // resolve the dialer's ARQ, echo mode+ISN
        if(m_on_accepted)
            m_on_accepted(adopt_accepted(raw));
    }

    void resolve_paired(udp_channel *ch)
    {
        if(auto *arq = m_dials.payload_of(ch))
            (*arq)->on_paired_frame();
    }

    void resolve_dial(const io::endpoint &ep, udp_channel *raw)
    {
        // COPY ep before resolve erases the entry: ep is bound to the pending entry's
        // ARQ-closure capture, which the erase destroys — re-emitting the freed reference
        // is a use-after-free (an on_dialed consumer that copies the endpoint reads it).
        const io::endpoint dialed = ep;
        auto ch = m_dials.resolve(raw);
        if(!ch)
            return;
        if(m_on_dialed)
            m_on_dialed(std::move(ch), dialed);
    }

    void fail_dial(const io::endpoint &ep, udp_channel *raw)
    {
        // COPY ep AND the dest out before fail erases the entry, for the same reason as
        // resolve_dial: the pending entry owns the ARQ closure that ep is bound to, and
        // the channel (whose dest the demux is keyed on) is moved out by fail().
        const io::endpoint failed = ep;
        m_demux.erase(raw->dest());
        m_dials.fail(raw);                  // routes the freed channel through the deferred-destroy sink
        report_dial_fail(failed, io::io_error::timed_out);
    }

    // Transfer ownership of an accepted channel out to the on_accepted callback while
    // leaving the demux raw ref intact (the engine owns the channel; the demux keeps
    // routing inbound datagrams to it by endpoint).
    std::unique_ptr<udp_channel> adopt_accepted(udp_channel *raw)
    {
        return m_dials.adopt_accepted(raw);
    }

    // Close the borrow-vs-own footgun: the engine owns the handed-out channel but the
    // demux keeps a non-owning ref, so when the engine destroys the channel the demux
    // must drop its ref or the next datagram dereferences freed memory. The channel's
    // dtor fires this seam (distinct from the consumer on_closed/on_error the engine
    // overwrites); the identity-guarded erase leaves a same-endpoint re-dial untouched.
    void wire_teardown(udp_channel &ch, const endpoint_type &key)
    {
        ch.on_teardown([this, key, raw = &ch] { m_demux.erase_if_matches(key, raw); });
    }

    void send_handshake(const endpoint_type &dest, hs_type type,
                        io::detail::udp_channel_mode mode = io::detail::udp_channel_mode::best_effort,
                        std::uint16_t initial_seq = 0)
    {
        io::detail::encode_handshake_into(m_hs_scratch, type, mode, initial_seq);
        m_server.send_to(m_hs_scratch, dest);
    }

    // The scheme -> channel-mode classifier: "udpr" requests the reliable-datagram ARQ
    // class; every other scheme (including bare "udp") is best_effort. The mirror of the
    // mux selector's reliability_of_scheme, applied at the datagram member's dial face.
    [[nodiscard]] static io::detail::udp_channel_mode mode_of_scheme(const std::string &scheme) noexcept
    {
        return scheme == "udpr" ? io::detail::udp_channel_mode::reliable_datagram
                                : io::detail::udp_channel_mode::best_effort;
    }

    // Draw a random per-session ISN (RFC 6528 lineage) from the setup-time-seeded PRNG —
    // a setup-time event (one draw per dial/accept), NOT a per-packet RNG. The range omits
    // 0 so a negotiated ISN is always distinguishable from the legacy back-compat default
    // (an absent ISN field decodes 0) and the spoof-resistance property always holds.
    std::uint16_t next_isn()
    {
        return std::uniform_int_distribution<std::uint16_t>{1, 0xFFFF}(m_isn_rng);
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e) { if(m_on_dial_failed) m_on_dial_failed(ep, e); }
    void report_error(io::io_error e) { if(m_on_error) m_on_error(e); }

    ::asio::io_context &m_io;
    udp_server m_server;
    detail::udp_inbound_demux m_demux;
    std::size_t m_max_payload;
    arq_type::schedule m_hs_ladder;
    io::detail::udp_arq_config m_arq_cfg;
    io::congestion m_congestion;
    std::mt19937 m_isn_rng;                  // setup-time-seeded; one draw per dial/accept (no hot-path RNG)
    std::vector<std::byte> m_hs_scratch;
    dial_registry m_dials;                  // the half-open dial table + the accepted table
    plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::udp_transport, plexus::asio::udp_policy>,
    "udp_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
