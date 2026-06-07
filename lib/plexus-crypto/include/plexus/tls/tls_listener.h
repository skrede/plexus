#ifndef HPP_GUARD_PLEXUS_TLS_TLS_LISTENER_H
#define HPP_GUARD_PLEXUS_TLS_TLS_LISTENER_H

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <string>
#include <memory>
#include <variant>
#include <utility>
#include <cstdint>
#include <system_error>

namespace plexus::tls {

// TCP acceptor minting server-handshake tls_channels. start(endpoint) opens,
// binds, and listens over ::asio::ip::tcp::acceptor (TLS rides TCP — no
// socket-file lifecycle), then loops accepting; each accepted tcp::socket is
// adopted by a fresh tls_channel (accept ctor) that runs the SERVER handshake
// internally before arming its read loop — the listener does not block on it.
// The credential is REQUIRED, non-defaultable (a node without a credential
// cannot stand up a TLS transport); cfg stays required-WITH-default.
class tls_listener
{
public:
    tls_listener(::asio::io_context &io, const tls_credential &cred,
                 wire::stream_inbound_config cfg = {})
        : m_io(io)
        , m_acceptor(io)
        , m_cred(cred)
        , m_cfg(cfg)
        , m_accepting([this](std::unique_ptr<tls_channel> ch) { defer_destroy(std::move(ch)); })
    {
    }

    tls_listener(const tls_listener &) = delete;
    tls_listener &operator=(const tls_listener &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void start(const io::endpoint &bind_ep)
    {
        std::error_code ec;
        auto ep = plexus::asio::detail::parse(bind_ep.address, ec);
        if(ec)
            return report(ec);
        m_acceptor.open(ep.protocol(), ec);
        if(ec)
            return report(ec);
        (void)m_acceptor.set_option(::asio::socket_base::reuse_address(true), ec);
        m_acceptor.bind(ep, ec);
        if(ec)
            return report(ec);
        m_acceptor.listen(::asio::socket_base::max_listen_connections, ec);
        if(ec)
            return report(ec);
        m_running = true;
        do_accept();
    }

    void stop()
    {
        m_running = false;
        std::error_code ec;
        (void)m_acceptor.cancel(ec);
        (void)m_acceptor.close(ec);
        m_accepting.clear();
    }

    [[nodiscard]] uint16_t port() const
    {
        std::error_code ec;
        auto ep = m_acceptor.local_endpoint(ec);
        return ec ? 0u : ep.port();
    }

private:
    void do_accept()
    {
        m_acceptor.async_accept(
            [this](std::error_code ec, ::asio::ip::tcp::socket peer)
            {
                if(ec)
                {
                    if(ec != ::asio::error::operation_aborted)
                        report(ec);
                    return;
                }
                run_server_handshake(
                    std::make_unique<tls_channel>(m_io, std::move(peer), m_cred, m_cfg));
                if(m_running)
                    do_accept();
            });
    }

    // Run the server handshake before delivering the channel: the accepted channel is
    // owned by the registry's accepted table across the handshake (never by its own
    // readiness closure), so a verify reject — which fires the channel's error/fail edge
    // from inside its own async stack — drops it through the registry's deferred-destroy
    // (freed OFF that stack) and the consumer never receives a verify-rejected peer's
    // channel (fail-closed, symmetric with the dial side's post-handshake delivery).
    void run_server_handshake(std::unique_ptr<tls_channel> ch)
    {
        auto *raw = ch.get();
        m_accepting.insert_accepted(raw, std::move(ch));
        raw->on_error([this, raw](io::io_error) { drop_accept(raw); });
        raw->start_server_handshake([this, raw]() mutable { resolve_accept(raw); });
    }

    // The server handshake succeeded: adopt the channel OUT of the accepted table and
    // hand it to on_accepted.
    void resolve_accept(tls_channel *raw)
    {
        auto ch = m_accepting.adopt_accepted(raw);
        if(ch && m_on_accepted)
            m_on_accepted(std::move(ch));
    }

    // A handshake/verify failure: drop the accepted channel OFF its own async stack via a
    // posted continuation (the deferred-destroy sink), never synchronously mid-call.
    void drop_accept(tls_channel *raw)
    {
        if(auto ch = m_accepting.adopt_accepted(raw))
            defer_destroy(std::move(ch));
    }

    // Destroy a freed channel OFF the current stack: a posted continuation owns it until
    // it runs, so it is torn down after the channel's own async-completion call unwinds.
    void defer_destroy(std::unique_ptr<tls_channel> ch)
    {
        ::asio::post(m_io, [owned = std::move(ch)]() mutable { owned.reset(); });
    }

    void report(const std::error_code &ec)
    {
        if(m_on_error)
            m_on_error(plexus::asio::detail::map_error(ec));
    }

    ::asio::io_context &m_io;
    ::asio::ip::tcp::acceptor m_acceptor;
    const tls_credential &m_cred;
    wire::stream_inbound_config m_cfg;
    io::pending_dial_registry<tls_channel, std::monostate> m_accepting;   // accepted-table owner
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_running{false};
};

}

#endif
