#ifndef HPP_GUARD_PLEXUS_TLS_TLS_TRANSPORT_H
#define HPP_GUARD_PLEXUS_TLS_TLS_TRANSPORT_H

#include "plexus/tls/tls_policy.h"
#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_listener.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/tls_context.h"

#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <array>
#include <memory>
#include <variant>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::tls {

// The TLS connector: a protocol-type swap of the plaintext transport with a
// handshake hop. wraps tls_listener for listen + accept and adds an
// async-connect-then-client-handshake dial over the ssl::stream. listen(ep)
// starts the acceptor and forwards each accepted (server-handshaking) channel
// through on_accepted; dial(ep) parses the host:port target, async-connects a
// fresh client channel, then runs the client handshake and hands the live
// channel to on_dialed ONLY POST-handshake — so a verify-rejected peer never
// yields a live channel (fail-closed). Each half-open dial is owned by the
// transport's pending_dial_registry (copy-before-erase resolve, deferred-destroy
// fail) — never by the channel's own readiness closure — so a fail edge firing
// from inside the channel's async stack frees it OFF that stack. A handshake/verify
// failure surfaces as on_dial_failed via the channel's own error path. The credential is REQUIRED,
// non-defaultable (first after io); cfg stays required-WITH-default. The dialed
// endpoint rides the async closure so on_dialed / on_dial_failed CARRY it back
// (the engine correlates each completion to its slot by endpoint).
class tls_transport
{
public:
    // no_delay disables Nagle on every dialed + accepted TCP lowest layer (required-WITH-
    // default true — the latency-MW default; overridable for a Nagle use-case). Threaded to
    // the listener for the accept side and set on the dial socket post-connect below.
    tls_transport(::asio::io_context &io, const tls_credential &cred,
                  wire::stream_inbound_config cfg = {}, bool no_delay = true,
                  io::congestion congestion = io::congestion::block,
                  std::size_t outbox_bytes = tls_channel::default_outbox_bytes,
                  plexus::asio::stream_socket_options socket_options = {})
        : m_io(io)
        , m_cred(cred)
        , m_listener(io, cred, cfg, no_delay, congestion, outbox_bytes, socket_options)
        , m_cfg(cfg)
        , m_no_delay(no_delay)
        , m_congestion(congestion)
        , m_outbox_bytes(outbox_bytes)
        , m_socket_options(socket_options)
        , m_pending([this](std::unique_ptr<tls_channel> ch) { defer_destroy(std::move(ch)); })
    {
        m_listener.on_accepted([this](std::unique_ptr<tls_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(std::move(ch));
        });
        m_listener.on_error([this](io::io_error e) {
            if(m_on_error)
                m_on_error(e);
        });
    }

    tls_transport(const tls_transport &) = delete;
    tls_transport &operator=(const tls_transport &) = delete;

    // The concrete channel this member's completions deliver + its routing identity: the
    // schemes it serves and the locality tier. TLS-over-TCP is a remote (encrypted stream)
    // member serving the "tls" scheme — it rides a network path, so locality confinement
    // still excludes it. A generic multiplexer reads these at compile time to route over a pack.
    using channel_type = tls_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"tls"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep) { m_listener.start(ep); }

    [[nodiscard]] uint16_t port() const { return m_listener.port(); }

    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto target = plexus::asio::detail::parse(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, plexus::asio::detail::map_error(pec));
        auto ch = std::make_unique<tls_channel>(m_io, m_cred, m_cfg, m_congestion, m_outbox_bytes, m_socket_options);
        auto *raw = ch.get();
        raw->socket().async_connect(target,
            [this, ep, ch = std::move(ch), raw](std::error_code ec) mutable {
                if(ec)
                    return report_dial_fail(ep, plexus::asio::detail::map_error(ec));
                if(m_no_delay)
                {
                    std::error_code nec;
                    (void)raw->socket().set_option(::asio::ip::tcp::no_delay(true), nec);   // disable Nagle pre-handshake
                }
                run_handshake(std::move(ch), raw, ep);
            });
    }

    void close()
    {
        m_pending.clear();
        m_listener.stop();
    }

private:
    // Run the client handshake; deliver to on_dialed only on success. The minted
    // channel is owned by the registry (transport-owned) across the handshake — no
    // self-owning readiness closure. A handshake/verify failure routes through the
    // channel's on_error to fail_dial (deferred-destroy + on_dial_failed, fail-closed).
    void run_handshake(std::unique_ptr<tls_channel> ch, tls_channel *raw, const io::endpoint &ep)
    {
        m_pending.insert(raw, std::move(ch));
        raw->on_error([this, ep, raw](io::io_error e) { fail_dial(ep, raw, e); });
        auto host = detail::sni_host(ep.address);
        raw->start_client_handshake(host, [this, ep, raw]() mutable { resolve_dial(ep, raw); });
    }

    // The handshake succeeded: resolve the channel OUT of the registry and deliver it.
    // Copy ep before resolve()/erase — ep is bound to the readiness closure capture the
    // erase destroys (copy-before-erase). The consumer re-wires on_error when it adopts
    // the channel from on_dialed (the established adopt-then-wire contract).
    void resolve_dial(const io::endpoint &ep, tls_channel *raw)
    {
        const io::endpoint dialed = ep;
        auto ch = m_pending.resolve(raw);
        if(ch && m_on_dialed)
            m_on_dialed(std::move(ch), dialed);
    }

    // A handshake/verify failure: drop the dial through the registry's deferred-destroy
    // (the channel is freed OFF its own async stack, not synchronously mid-call), then
    // report on_dial_failed. Copy ep before fail()/erase (copy-before-erase).
    void fail_dial(const io::endpoint &ep, tls_channel *raw, io::io_error e)
    {
        const io::endpoint failed = ep;
        m_pending.fail(raw);
        report_dial_fail(failed, e);
    }

    // Destroy a freed channel OFF the current stack: a posted continuation owns it until
    // it runs, so it is torn down after the channel's own async-completion call unwinds.
    void defer_destroy(std::unique_ptr<tls_channel> ch)
    {
        ::asio::post(m_io, [owned = std::move(ch)]() mutable { owned.reset(); });
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    ::asio::io_context &m_io;
    const tls_credential &m_cred;
    tls_listener m_listener;
    wire::stream_inbound_config m_cfg;
    bool m_no_delay;
    io::congestion m_congestion;
    std::size_t m_outbox_bytes;
    plexus::asio::stream_socket_options m_socket_options;
    io::pending_dial_registry<tls_channel, std::monostate> m_pending;   // transport-owned half-open dials
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::tls::tls_transport, plexus::tls::tls_policy>,
    "tls_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
