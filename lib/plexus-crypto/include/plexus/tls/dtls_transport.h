#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_TRANSPORT_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_TRANSPORT_H

#include "plexus/tls/dtls_policy.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_inbound_demux.h"
#include "plexus/asio/detail/asio_udp_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <memory>
#include <string>
#include <variant>
#include <utility>
#include <cstddef>
#include <string_view>
#include <system_error>

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
//   dial(ep): mint a client channel into the transport-owned pending-dial registry,
//     kick the client handshake, and fire on_dialed ON external_complete (the crypto
//     handshake IS the plexus handshake — no wire round-trip). A verify /
//     retransmit-timeout failure fires on_dial_failed and fails the pending entry
//     (deferred-destroy off the channel's own stack).
//   close(): clear the registry (both tables), reset the demux, close the socket —
//     io_context teardown then destroys every in-flight channel cleanly (the leak
//     the self-owning-channel cycle had; transport-owned dials close it).
//
// Single-owner: the registry holds unique_ptr<dtls_channel> (transport-owned) keyed by
// raw pointer. There is NO self-owning channel cycle (the release_pending pattern is the
// hazard this transport AVOIDS). The registry bakes the copy-before-erase + deferred-
// destroy lifetime contracts the hand-inlined maps used to carry inline.
class dtls_transport
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    explicit dtls_transport(::asio::io_context &io, const tls_credential &cred,
                            std::size_t max_payload = dtls_channel::default_max_payload)
        : m_io(io)
        , m_server(io)
        , m_cred(cred)
        , m_cookie(make_cookie_secret())
        , m_max_payload(max_payload)
        , m_registry(make_defer_destroy())
    {
        m_server.on_datagram([this](const endpoint_type &from, std::span<const std::byte> bytes) { on_datagram(from, bytes); });
        m_server.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
    }

    dtls_transport(const dtls_transport &) = delete;
    dtls_transport &operator=(const dtls_transport &) = delete;

    ~dtls_transport() { close(); }

    // The concrete channel this member's completions deliver + its routing identity: the
    // schemes it serves and the locality tier. DTLS-over-UDP is a remote (encrypted
    // datagram) member serving the "dtls" scheme — it rides a network path, so locality
    // confinement still excludes it. A generic multiplexer reads these at compile time to
    // route over a pack.
    using channel_type = dtls_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"dtls"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    // The drop-observability seam (null by default — zero cost when unobserved). The
    // per-peer demux cap refusal emits demux_refused here, the cross-transport twin of the
    // plain-UDP seam. The sink POSTS, so the spoof-flood refusal never fires inline.
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb) { m_on_drop = std::move(cb); }

    void listen(const io::endpoint &ep)
    {
        std::error_code pec;
        auto bind_ep = plexus::asio::detail::parse_udp(ep.address, pec);
        if(pec)
            return report_error(plexus::asio::detail::map_error(pec));
        m_server.start(bind_ep);
    }

    // dial mints a client channel into the pending-dial registry (transport-owned), kicks the client
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
        wire_teardown(*raw, dest);

        raw->on_external_complete([this, ep, raw] { resolve_dial(ep, raw); });
        raw->on_error([this, ep, raw](io::io_error e) { fail_dial(ep, raw, e); });

        m_registry.insert(raw, std::move(ch));
        raw->start_handshake();
    }

    void close()
    {
        // clear() drops both registry tables, destroying each held channel synchronously
        // from this own-close path (never from inside a channel's member call).
        m_registry.clear();
        m_demux = plexus::asio::detail::basic_inbound_demux<dtls_channel>{};
        m_server.close();
    }

    [[nodiscard]] std::uint16_t port() const { return m_server.port(); }

private:
    using dial_registry = io::pending_dial_registry<dtls_channel, std::monostate>;

    // The deferred-destroy sink the registry routes a failed channel through (both the
    // dial fail and the accept-side fail_accepted): a fail edge may fire from inside the
    // channel's own deliver_inbound/drain stack (the triggering datagram is fed straight
    // into a freshly-minted accept channel), so destroying it there frees it mid-unwind.
    // Posting it to a continuation that owns it until it runs defers the destruction off
    // the current stack.
    dial_registry::defer_destroy make_defer_destroy()
    {
        return [this](std::unique_ptr<dtls_channel> ch)
        {
            ::asio::post(m_io, [owned = std::move(ch)]() mutable { owned.reset(); });
        };
    }

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
        {
            if(m_on_drop)
                m_on_drop(io::detail::drop_event{.cause = io::detail::drop_cause::demux_refused,
                                                 .transport = io::locality::remote});
            return;                                        // peer cap reached: drop the flood
        }
        wire_teardown(*raw, from);
        m_registry.insert_accepted(raw, std::move(ch));
        raw->on_external_complete([this, raw] { resolve_accept(raw); });
        raw->on_error([this, raw](io::io_error) { drop_accept(raw); });
        raw->start_handshake();
        raw->deliver_inbound(bytes);                       // feed the triggering ClientHello
    }

    void resolve_dial(const io::endpoint &ep, dtls_channel *raw)
    {
        // COPY ep before resolve erases the entry: ep is bound to the pending entry's
        // closure capture, which the erase destroys — re-emitting the freed reference is
        // a use-after-free (an on_dialed consumer that copies the endpoint reads it).
        const io::endpoint dialed = ep;
        auto ch = m_registry.resolve(raw);
        if(!ch)
            return;
        if(m_on_dialed)
            m_on_dialed(std::move(ch), dialed);
    }

    void fail_dial(const io::endpoint &ep, dtls_channel *raw, io::io_error e)
    {
        // COPY ep AND erase the demux ref before fail() moves the channel out: fail()
        // fired this from INSIDE raw's own deliver_inbound/drain_inbound stack, so the
        // registry routes the freed channel through the deferred-destroy sink (never a
        // synchronous destruction mid-unwind). Detach the demux ref now (no more inbound
        // routes here), then fail() the entry.
        const io::endpoint failed = ep;
        m_demux.erase(raw->dest());
        m_registry.fail(raw);
        // Thread the channel's actual error through (a verify/pin failure surfaces as
        // connection_refused, a broken pipe as broken_pipe, a retransmit-exhaustion
        // timeout as timed_out) so the consumer can distinguish "peer never answered"
        // from "peer presented an unpinned cert" — diagnostics + retry-policy material.
        report_dial_fail(failed, e);
    }

    // Close the borrow-vs-own footgun: the engine owns the handed-out channel but the
    // demux keeps a non-owning ref, so when the engine destroys the channel the demux
    // must drop its ref or the next datagram dereferences freed memory. The channel's
    // dtor fires this seam (distinct from the consumer on_closed/on_error the engine
    // overwrites); the identity-guarded erase leaves a same-endpoint re-dial untouched.
    void wire_teardown(dtls_channel &ch, const endpoint_type &key)
    {
        ch.on_teardown([this, key, raw = &ch] { m_demux.erase_if_matches(key, raw); });
    }

    // An accepted server channel completed its mutual handshake: hand it to
    // on_accepted while the demux raw ref keeps routing inbound datagrams to it.
    void resolve_accept(dtls_channel *raw)
    {
        auto ch = m_registry.adopt_accepted(raw);
        if(!ch)
            return;
        if(m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // A handshake/verify failure on an accept-side channel: drop it (the source never
    // proved a pinned identity — fail-closed, no accepted channel surfaces). Same
    // self-destruction hazard as fail_dial — fail_accepted routes the freed channel
    // through the deferred-destroy sink; detach the demux ref now.
    void drop_accept(dtls_channel *raw)
    {
        m_demux.erase(raw->dest());
        m_registry.fail_accepted(raw);
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e) { if(m_on_dial_failed) m_on_dial_failed(ep, e); }
    void report_error(io::io_error e) { if(m_on_error) m_on_error(e); }

    ::asio::io_context &m_io;
    plexus::asio::udp_server m_server;
    const tls_credential &m_cred;
    io::security::cookie_secret m_cookie;                  // the node's single cookie secret
    std::size_t m_max_payload;
    plexus::asio::detail::basic_inbound_demux<dtls_channel> m_demux;
    dial_registry m_registry;                             // the half-open dial table + the accepted table
    plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop;
};

}

static_assert(plexus::io::transport_backend<plexus::tls::dtls_transport, plexus::tls::dtls_policy>,
    "dtls_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
