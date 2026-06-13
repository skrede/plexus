#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_BOOTSTRAP_H

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/tls_context.h"

#include "plexus/io/detail/handshake_gate.h"
#include "plexus/wire_bytes.h"
#include "plexus/detail/compat.h"

#include <asio/ssl/stream.hpp>
#include <asio/ssl/context.hpp>

#include <openssl/ssl.h>

#include <span>
#include <string>
#include <utility>
#include <cstddef>

namespace plexus::tls::detail {

// The TLS open-before-data path: the read loop arms ONLY on handshake success (so
// ciphertext is never read as frames) and outbound application sends route through an
// io::detail::handshake_gate that BUFFERS them until mark_ready, then drains them to the
// channel's serial egress — plaintext is structurally never written before the secure
// channel is up. This is the plaintext bootstrap shape (make_stream / submit / arm_read)
// with the gate + the ssl::context spliced in; the handshake hooks live here, not on the
// channel core. The gate's drain sink is bound lazily (the channel that owns this bootstrap
// installs it after construction) so the gate can forward into the channel's egress.
template <typename Stream>
class tls_bootstrap
{
public:
    explicit tls_bootstrap(const tls_credential &cred)
        : m_ssl_ctx(share_context(cred))
        , m_gate([this](std::span<const std::byte> bytes) { if(m_drain) m_drain(bytes); })
    {
    }

    template <typename Io>
    [[nodiscard]] Stream make_stream(Io &io) { return Stream{io, m_ssl_ctx}; }

    template <typename Io, typename Socket>
    [[nodiscard]] Stream make_stream(Io &io, Socket connected)
    {
        (void)io;
        return Stream{std::move(connected), m_ssl_ctx};
    }

    // The owning channel binds the drain (its egress) and routes its send() here. Pre-ready
    // the gate buffers an owned copy; post-ready it forwards straight through to the egress.
    template <typename Channel>
    void submit(Channel &, std::span<const std::byte> data) { m_gate.submit(data); }

    // The TLS open-before-data gate buffers an owned copy pre-handshake and forwards
    // post-handshake, so it cannot hold a borrowed owner across the handshake window; the
    // owner overload therefore submits the owner's view (the gate copies) — byte-identical
    // to the span path. The owner is released when this returns.
    template <typename Channel>
    void submit(Channel &, plexus::wire_bytes<> data)
    {
        m_gate.submit(static_cast<std::span<const std::byte>>(data));
    }

    // An accepted TLS channel does NOT read at accept — the server handshake (started by
    // the listener) arms the read loop. arm_on_accept is the no-op the channel calls in the
    // same slot the plaintext bootstrap starts reading.
    template <typename Channel>
    void arm_on_accept(Channel &) {}

    void bind_drain(io::detail::handshake_gate::drain_fn drain) { m_drain = std::move(drain); }

    // Dial-side bootstrap: set SNI (best-effort) then run the client handshake. on_ready
    // fires ONCE the secure channel is up, so the transport delivers the channel POST-
    // handshake — a verify-rejected peer never yields a live one (fail-closed).
    template <typename Channel>
    void start_client_handshake(Channel &c, const std::string &host,
                                plexus::detail::move_only_function<void()> on_ready)
    {
        c.mark_open();
        m_on_ready = std::move(on_ready);
        if(!host.empty())
            (void)::SSL_set_tlsext_host_name(c.stream().native_handle(), host.c_str());
        do_handshake(c, ::asio::ssl::stream_base::client);
    }

    // Accept-side bootstrap: run the server handshake; symmetric fail-closed delivery.
    template <typename Channel>
    void start_server_handshake(Channel &c, plexus::detail::move_only_function<void()> on_ready)
    {
        c.mark_open();
        m_on_ready = std::move(on_ready);
        do_handshake(c, ::asio::ssl::stream_base::server);
    }

    void reset() { m_gate.reset(); }

private:
    template <typename Channel>
    void do_handshake(Channel &c, ::asio::ssl::stream_base::handshake_type mode)
    {
        c.stream().async_handshake(mode, [this, &c](std::error_code ec) {
            if(ec)
                return c.fail(ec);
            c.start_read_loop();
            m_gate.mark_ready();   // drain whatever buffered while the handshake was in flight
            if(m_on_ready)
                m_on_ready();
        });
    }

    ::asio::ssl::context m_ssl_ctx;
    io::detail::handshake_gate m_gate;                 // open-before-data outbound buffer
    io::detail::handshake_gate::drain_fn m_drain;
    plexus::detail::move_only_function<void()> m_on_ready;
};

}

#endif
