#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_TRANSPORT_H

#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/udp_inbound_demux.h"
#include "plexus/asio/detail/udp_handshake_arq.h"
#include "plexus/asio/detail/udp_handshake_frame.h"
#include "plexus/asio/detail/asio_udp_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <vector>
#include <utility>
#include <cstddef>
#include <optional>
#include <system_error>
#include <unordered_map>

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
    using arq_type = detail::udp_handshake_arq<udp_policy>;
    using hs_type = detail::udp_hs_type;

    explicit udp_transport(::asio::io_context &io, std::size_t max_payload = udp_channel::default_max_payload,
                           arq_type::schedule hs_ladder = arq_type::default_ladder)
        : m_io(io)
        , m_server(io)
        , m_max_payload(max_payload)
        , m_hs_ladder(hs_ladder)
    {
        m_server.on_datagram([this](const endpoint_type &from, std::span<const std::byte> bytes) { on_datagram(from, bytes); });
        m_server.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
    }

    udp_transport(const udp_transport &) = delete;
    udp_transport &operator=(const udp_transport &) = delete;

    ~udp_transport() { close(); }

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
    // the ARQ closures so the engine correlates the completion by endpoint.
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto dest = detail::parse_udp(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, detail::map_error(pec));

        ensure_bound(dest.protocol());      // a dial-only transport still needs a bound socket to send/recv

        auto ch = std::make_unique<udp_channel>(m_io, m_server, dest, m_max_payload);
        auto *raw = ch.get();
        m_demux.insert(dest, raw);

        auto arq = std::make_unique<arq_type>(m_io, m_hs_ladder);
        auto *raw_arq = arq.get();
        raw_arq->on_transmit([this, dest] { send_handshake(dest, hs_type::request); });
        raw_arq->on_established([this, ep, raw] { resolve_dial(ep, raw); });
        raw_arq->on_timeout([this, ep, raw] { fail_dial(ep, raw); });

        m_pending[raw] = pending_dial{std::move(ch), std::move(arq)};
        raw_arq->start();
    }

    void close()
    {
        for(auto &[raw, pend] : m_pending)
            pend.arq->cancel();
        m_pending.clear();
        m_accepted.clear();
        m_demux = detail::udp_inbound_demux{};
        m_server.close();
    }

    [[nodiscard]] std::uint16_t port() const { return m_server.port(); }

private:
    struct pending_dial
    {
        std::unique_ptr<udp_channel> channel;
        std::unique_ptr<arq_type>    arq;
    };

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
        if(auto hs = detail::decode_handshake(bytes))
        {
            if(*hs == hs_type::response)
                resolve_paired(ch);
            else
                send_handshake(from, hs_type::response);   // re-ack a retransmitted request
            return;
        }
        ch->deliver_inbound(bytes);
    }

    // A never-seen source: ONLY a handshake request synthesizes an accept (the source
    // endpoint is not trusted as identity — bare data from an unknown source is
    // dropped). The demux cap bounds the spoof-flood (T-15-03).
    void accept_new_peer(const endpoint_type &from, std::span<const std::byte> bytes)
    {
        auto hs = detail::decode_handshake(bytes);
        if(!hs || *hs != hs_type::request)
            return;
        auto ch = std::make_unique<udp_channel>(m_io, m_server, from, m_max_payload);
        auto *raw = ch.get();
        if(!m_demux.insert(from, raw))
            return;                                        // peer cap reached: drop the flood
        m_accepted[raw] = std::move(ch);
        send_handshake(from, hs_type::response);           // let the dialer's ARQ resolve
        if(m_on_accepted)
            m_on_accepted(adopt_accepted(raw));
    }

    void resolve_paired(udp_channel *ch)
    {
        auto it = m_pending.find(ch);
        if(it != m_pending.end())
            it->second.arq->on_paired_frame();
    }

    void resolve_dial(const io::endpoint &ep, udp_channel *raw)
    {
        auto it = m_pending.find(raw);
        if(it == m_pending.end())
            return;
        // COPY ep before the erase: ep is bound to the pending entry's ARQ-closure
        // capture, which erase() destroys — re-emitting the freed reference is a
        // use-after-free (an on_dialed consumer that copies the endpoint reads it).
        const io::endpoint dialed = ep;
        auto ch = std::move(it->second.channel);
        m_pending.erase(it);
        if(m_on_dialed)
            m_on_dialed(std::move(ch), dialed);
    }

    void fail_dial(const io::endpoint &ep, udp_channel *raw)
    {
        auto it = m_pending.find(raw);
        if(it == m_pending.end())
            return;
        // COPY ep before the erase, for the same reason as resolve_dial: the pending
        // entry owns the ARQ closure that ep is bound to.
        const io::endpoint failed = ep;
        m_demux.erase(it->second.channel->dest());
        m_pending.erase(it);
        report_dial_fail(failed, io::io_error::timed_out);
    }

    // Transfer ownership of an accepted channel out to the on_accepted callback while
    // leaving the demux raw ref intact (the engine owns the channel; the demux keeps
    // routing inbound datagrams to it by endpoint).
    std::unique_ptr<udp_channel> adopt_accepted(udp_channel *raw)
    {
        auto it = m_accepted.find(raw);
        auto ch = std::move(it->second);
        m_accepted.erase(it);
        return ch;
    }

    void send_handshake(const endpoint_type &dest, hs_type type)
    {
        detail::encode_handshake_into(m_hs_scratch, type);
        m_server.send_to(m_hs_scratch, dest);
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e) { if(m_on_dial_failed) m_on_dial_failed(ep, e); }
    void report_error(io::io_error e) { if(m_on_error) m_on_error(e); }

    ::asio::io_context &m_io;
    udp_server m_server;
    detail::udp_inbound_demux m_demux;
    std::size_t m_max_payload;
    arq_type::schedule m_hs_ladder;
    std::vector<std::byte> m_hs_scratch;
    std::unordered_map<udp_channel *, pending_dial> m_pending;
    std::unordered_map<udp_channel *, std::unique_ptr<udp_channel>> m_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<udp_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::udp_transport, plexus::asio::udp_policy>,
    "udp_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
