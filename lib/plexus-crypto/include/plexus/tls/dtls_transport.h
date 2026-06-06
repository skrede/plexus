#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_TRANSPORT_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_TRANSPORT_H

#include "plexus/tls/dtls_policy.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/udp_inbound_demux.h"
#include "plexus/asio/detail/asio_udp_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <string>
#include <utility>
#include <cstddef>
#include <system_error>
#include <unordered_map>

namespace plexus::tls {

// The secure-best_effort DTLS transport_backend: owns the ONE bound udp_server, the
// inbound demux (sender host:port -> dtls_channel), the node's single cookie secret,
// the transport-owned pending-dial table, and every minted dtls_channel. It mints
// logical secure channels over the shared socket — there is NO acceptor and NO
// per-peer accept socket. DTLS records ride RAW (no plexus udp_envelope, no dedup,
// no ARQ — DTLS owns RFC-6347 anti-replay + handshake retransmit, D-03).
//
//   listen(ep): bind the shared server, arm the recv loop. Each datagram is
//     demuxed by sender: a HIT routes to deliver_inbound; a MISS from a never-seen
//     source mints a server-side channel behind the cookie gate (OpenSSL replies
//     HelloVerifyRequest and allocates no full state until a valid cookie echoes),
//     feeds the triggering datagram, and fires on_accepted on completion.
//   dial(ep): mint a client channel into the TRANSPORT-OWNED m_pending table, kick
//     the client handshake, and fire on_dialed ON external_complete (the crypto
//     handshake IS the plexus handshake — no wire round-trip, D-01). A verify /
//     retransmit-timeout failure fires on_dial_failed and erases the pending entry.
//   close(): clear m_pending, then m_accepted, reset the demux, close the socket —
//     io_context teardown then destroys every in-flight channel cleanly (the leak
//     the TLS self-owning-channel cycle had; transport-owned dials close it).
//
// Single-owner: m_pending holds unique_ptr<dtls_channel> (transport-owned). There is
// NO self-owning channel cycle (the tls_channel release_pending pattern is the
// hazard this transport AVOIDS — the tls-pending-dial-ownership structural fix).
class dtls_transport
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    explicit dtls_transport(::asio::io_context &io, const tls_credential &cred,
                            std::size_t max_payload = dtls_channel::default_max_payload)
        : m_io(io)
        , m_server(io)
        , m_cred(cred)
        , m_max_payload(max_payload)
    {
        m_server.on_datagram([this](const endpoint_type &from, std::span<const std::byte> bytes) { on_datagram(from, bytes); });
        m_server.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
    }

    dtls_transport(const dtls_transport &) = delete;
    dtls_transport &operator=(const dtls_transport &) = delete;

    ~dtls_transport() { close(); }

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep)
    {
        std::error_code pec;
        auto bind_ep = plexus::asio::detail::parse_udp(ep.address, pec);
        if(pec)
            return report_error(plexus::asio::detail::map_error(pec));
        m_server.start(bind_ep);
    }

    // dial mints a client channel into m_pending (transport-owned), kicks the client
    // DTLS handshake, and fires on_dialed ON external_complete — NOT immediately. ep
    // rides the closures so the engine correlates the completion by endpoint.
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto dest = plexus::asio::detail::parse_udp(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, plexus::asio::detail::map_error(pec));

        ensure_bound(dest.protocol());

        auto ch = std::make_unique<dtls_channel>(m_io, m_server, dest, m_cred, m_cookie,
                                                 dtls_channel::role::client, m_max_payload);
        auto *raw = ch.get();
        if(!m_demux.insert(dest, raw))
            return report_dial_fail(ep, io::io_error::address_in_use);

        raw->on_external_complete([this, ep, raw] { resolve_dial(ep, raw); });
        raw->on_error([this, ep, raw](io::io_error) { fail_dial(ep, raw); });

        m_pending[raw] = pending_dial{std::move(ch)};
        raw->start_handshake();
    }

    void close()
    {
        m_pending.clear();
        m_accepted.clear();
        m_demux = plexus::asio::detail::basic_inbound_demux<dtls_channel>{};
        m_server.close();
    }

    [[nodiscard]] std::uint16_t port() const { return m_server.port(); }

private:
    struct pending_dial
    {
        std::unique_ptr<dtls_channel> channel;
    };

    void ensure_bound(const ::asio::ip::udp &proto)
    {
        if(!m_server.is_open())
            m_server.start(endpoint_type(proto, 0));
    }

    void on_datagram(const endpoint_type &from, std::span<const std::byte> bytes)
    {
        if(auto *ch = m_demux.lookup(from))
            return ch->deliver_inbound(bytes);
        accept_new_peer(from, bytes);
    }

    // A never-seen source: mint a server-side channel behind the cookie gate. The
    // demux cap bounds the spoof-flood (drop the flood past the cap). OpenSSL drives
    // HelloVerifyRequest inside the accept state — no full handshake state until the
    // source echoes a valid cookie. The triggering datagram is fed AFTER minting
    // (OQ4). on_external_complete fires on_accepted (post mutual-verify, fail-closed).
    void accept_new_peer(const endpoint_type &from, std::span<const std::byte> bytes)
    {
        auto ch = std::make_unique<dtls_channel>(m_io, m_server, from, m_cred, m_cookie,
                                                 dtls_channel::role::server, m_max_payload);
        auto *raw = ch.get();
        if(!m_demux.insert(from, raw))
            return;                                        // peer cap reached: drop the flood
        m_accepted[raw] = std::move(ch);
        raw->on_external_complete([this, raw] { resolve_accept(raw); });
        raw->on_error([this, raw](io::io_error) { drop_accept(raw); });
        raw->start_handshake();
        raw->deliver_inbound(bytes);                       // feed the triggering ClientHello
    }

    void resolve_dial(const io::endpoint &ep, dtls_channel *raw)
    {
        auto it = m_pending.find(raw);
        if(it == m_pending.end())
            return;
        // COPY ep before the erase: ep is bound to the pending entry's closure
        // capture, which erase() destroys — re-emitting the freed reference is a UAF.
        const io::endpoint dialed = ep;
        auto ch = std::move(it->second.channel);
        m_pending.erase(it);
        if(m_on_dialed)
            m_on_dialed(std::move(ch), dialed);
    }

    void fail_dial(const io::endpoint &ep, dtls_channel *raw)
    {
        auto it = m_pending.find(raw);
        if(it == m_pending.end())
            return;
        const io::endpoint failed = ep;                    // COPY before erase (same reason)
        m_demux.erase(it->second.channel->dest());
        m_pending.erase(it);
        report_dial_fail(failed, io::io_error::timed_out);
    }

    // An accepted server channel completed its mutual handshake: hand it to
    // on_accepted while the demux raw ref keeps routing inbound datagrams to it.
    void resolve_accept(dtls_channel *raw)
    {
        auto it = m_accepted.find(raw);
        if(it == m_accepted.end())
            return;
        auto ch = std::move(it->second);
        m_accepted.erase(it);
        if(m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // A handshake/verify failure on an accept-side channel: drop it (the source
    // never proved a pinned identity — fail-closed, no accepted channel surfaces).
    void drop_accept(dtls_channel *raw)
    {
        auto it = m_accepted.find(raw);
        if(it == m_accepted.end())
            return;
        m_demux.erase(it->second->dest());
        m_accepted.erase(it);
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e) { if(m_on_dial_failed) m_on_dial_failed(ep, e); }
    void report_error(io::io_error e) { if(m_on_error) m_on_error(e); }

    ::asio::io_context &m_io;
    plexus::asio::udp_server m_server;
    const tls_credential &m_cred;
    dtls_cookie_state m_cookie;                            // the node's single cookie secret
    std::size_t m_max_payload;
    plexus::asio::detail::basic_inbound_demux<dtls_channel> m_demux;
    std::unordered_map<dtls_channel *, pending_dial> m_pending;
    std::unordered_map<dtls_channel *, std::unique_ptr<dtls_channel>> m_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::tls::dtls_transport, plexus::tls::dtls_policy>,
    "dtls_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
