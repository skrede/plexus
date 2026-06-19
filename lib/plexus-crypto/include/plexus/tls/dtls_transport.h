#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_TRANSPORT_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_TRANSPORT_H

#include "plexus/tls/dtls_policy.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/dtls_transport_accept.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_inbound_demux.h"
#include "plexus/asio/detail/asio_udp_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/fragmentation.h"
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

// over-limit: one cohesive secure DTLS transport_backend; the listen/dial/close verbs + the
// config the ctor binds all drive the shared m_server/m_demux/m_registry/m_cookie state (the
// transport_backend concept proof at file bottom binds the surface), so splitting the public face
// scatters that shared state — the listener/accept/cookie-exchange glue is extracted to
// detail/dtls_transport_accept.h.
//
// The secure-best_effort DTLS transport_backend: owns the ONE bound udp_server, the inbound demux
// (sender -> dtls_channel), the node's single cookie secret, the transport-owned pending-dial
// table, and every minted dtls_channel. DTLS records ride RAW (DTLS owns RFC-6347 anti-replay +
// handshake retransmit, D-03). listen(ep) arms the recv loop (a MISS mints a server channel behind
// the cookie gate — OpenSSL replies HelloVerifyRequest, no full state until a valid cookie echoes).
// dial(ep) kicks the client handshake, firing on_dialed ON external_complete (the crypto handshake
// IS the plexus handshake). Single-owner: the registry holds unique_ptr<dtls_channel> keyed by raw
// pointer, baking the copy-before-erase + deferred-destroy lifetime contracts.
class dtls_transport
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    // global_default is the per-MESSAGE size ceiling and reassembly_budget the aggregate
    // reassembly-memory cap — the two node-options message-size knobs the OTHER transports
    // already thread, brought to the encrypted best-effort lane so DTLS is tunable in lockstep
    // (a large best-effort DTLS message fragments per record under the always-on aggregate bound).
    // max_payload is the per-record logical budget, distinct from the message ceiling.
    explicit dtls_transport(::asio::io_context &io, const tls_credential &cred,
                            std::size_t max_payload       = dtls_channel::default_max_payload,
                            std::size_t global_default    = io::global_default_max_message_bytes,
                            std::size_t reassembly_budget = io::reassembly_memory_budget)
            : m_io(io)
            , m_server(io)
            , m_cred(cred)
            , m_cookie(make_cookie_secret())
            , m_max_payload(max_payload)
            , m_max_message_bytes(global_default)
            , m_reassembly_budget(reassembly_budget)
            , m_registry(make_defer_destroy())
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

    dtls_transport(const dtls_transport &)            = delete;
    dtls_transport &operator=(const dtls_transport &) = delete;

    ~dtls_transport() { close(); }

    // The concrete channel this member's completions deliver + its routing identity: the
    // schemes it serves and the locality tier. DTLS-over-UDP is a remote (encrypted
    // datagram) member serving the "dtls" scheme — it rides a network path, so locality
    // confinement still excludes it. A generic multiplexer reads these at compile time to
    // route over a pack.
    using channel_type = dtls_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"dtls"};
    static constexpr io::transport_kind              mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>,
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
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    // The drop-observability seam (null by default — zero cost when unobserved). The
    // per-peer demux cap refusal emits demux_refused here, the cross-transport twin of the
    // plain-UDP seam. The sink POSTS, so the spoof-flood refusal never fires inline.
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop = std::move(cb);
    }

    void listen(const io::endpoint &ep)
    {
        std::error_code pec;
        auto            bind_ep = plexus::asio::detail::parse_udp(ep.address, pec);
        if(pec)
            return detail::report_error(*this, plexus::asio::detail::map_error(pec));
        m_server.start(bind_ep);
    }

    // dial mints a client channel into the pending-dial registry (transport-owned), kicks the
    // client DTLS handshake, and fires on_dialed ON external_complete — NOT immediately. ep rides
    // the closures so the engine correlates the completion by endpoint.
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto            dest = plexus::asio::detail::parse_udp(ep.address, pec);
        if(pec)
            return detail::report_dial_fail(*this, ep, plexus::asio::detail::map_error(pec));

        detail::ensure_bound(*this, dest.protocol());

        auto ch = std::make_unique<dtls_channel>(
                m_io, m_server, dest, m_cred, m_cookie, dtls_channel::role::client, m_max_payload,
                dtls_channel::default_record_mtu, m_max_message_bytes, m_reassembly_budget);
        auto *raw = ch.get();
        if(!m_demux.insert(dest, raw))
            return detail::report_dial_fail(*this, ep, io::io_error::address_in_use);
        detail::wire_teardown(*this, *raw, dest);

        raw->on_external_complete([this, ep, raw] { detail::resolve_dial(*this, ep, raw); });
        raw->on_error([this, ep, raw](io::io_error e) { detail::fail_dial(*this, ep, raw, e); });

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
        { ::asio::post(m_io, [owned = std::move(ch)]() mutable { owned.reset(); }); };
    }

    // The listener/accept/cookie-exchange + dial-resolution glue is relocated to
    // detail/dtls_transport_accept.h (relocation by friendship): each helper reaches the
    // server/demux/dial-table members below through the transport reference.
    template<typename U>
    friend void detail::report_dial_fail(U &, const io::endpoint &, io::io_error);
    template<typename U>
    friend void detail::report_error(U &, io::io_error);
    template<typename U>
    friend void detail::ensure_bound(U &, const ::asio::ip::udp &);
    template<typename U>
    friend void detail::wire_teardown(U &, dtls_channel &, const typename U::endpoint_type &);
    template<typename U>
    friend void detail::resolve_accept(U &, dtls_channel *);
    template<typename U>
    friend void detail::drop_accept(U &, dtls_channel *);
    template<typename U>
    friend void detail::accept_new_peer(U &, const typename U::endpoint_type &,
                                        std::span<const std::byte>);
    template<typename U>
    friend void detail::on_datagram(U &, const typename U::endpoint_type &,
                                    std::span<const std::byte>);
    template<typename U>
    friend void detail::resolve_dial(U &, const io::endpoint &, dtls_channel *);
    template<typename U>
    friend void detail::fail_dial(U &, const io::endpoint &, dtls_channel *, io::io_error);

    ::asio::io_context         &m_io;
    plexus::asio::udp_server    m_server;
    const tls_credential       &m_cred;
    io::security::cookie_secret m_cookie; // the node's single cookie secret
    std::size_t                 m_max_payload;
    std::size_t                 m_max_message_bytes; // per-message ceiling threaded to each channel
    std::size_t m_reassembly_budget; // always-on aggregate reassembly-memory DoS cap
    plexus::asio::detail::basic_inbound_demux<dtls_channel> m_demux;
    dial_registry m_registry; // the half-open dial table + the accepted table
    plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<dtls_channel>, const io::endpoint &)>
                                                                                 m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)>                       m_on_error;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)>     m_on_drop;
};

}

static_assert(plexus::io::transport_backend<plexus::tls::dtls_transport, plexus::tls::dtls_policy>,
              "dtls_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
