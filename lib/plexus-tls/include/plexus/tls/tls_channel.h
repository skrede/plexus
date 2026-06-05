#ifndef HPP_GUARD_PLEXUS_TLS_TLS_CHANNEL_H
#define HPP_GUARD_PLEXUS_TLS_TLS_CHANNEL_H

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/tls_context.h"

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/write.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/ssl/context.hpp>

#include <openssl/ssl.h>

#include <deque>
#include <span>
#include <array>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::tls {

// byte_channel over ::asio::ssl::stream<tcp::socket>. A protocol-type swap of
// the plaintext stream channel: the bare socket becomes an ssl::stream and a
// handshake step is inserted before the read loop arms. Inbound (post-decrypt)
// bytes feed a member wire::stream_inbound exactly as TCP does — TLS is a
// reliable byte stream once the record layer decrypts — and each complete frame
// is POSTED to on_data; a framing violation or no-progress stall raises the
// distinct on_protocol_close seam. The channel is caller-owned, single-owner,
// runs its loops with bare `this` captured, and posts on_data — NO
// shared_from_this, NO strand (the prior art's lifetime is deliberately not
// ported). The read loop and the write-queue drain are gated on handshake
// completion so ciphertext is never read as frames and plaintext is never
// written before the secure channel is up.
class tls_channel
{
public:
    // Dial mode: unconnected ssl::stream. The transport async_connects the
    // lowest layer, then calls start_client_handshake(host).
    tls_channel(::asio::io_context &io, const tls_credential &cred,
                wire::stream_inbound_config cfg = {})
        : m_io(io)
        , m_ssl_ctx(detail::share_context(cred))
        , m_stream(io, m_ssl_ctx)
        , m_inbound(io, cfg)
    {
        wire_inbound();
    }

    // Accept mode: adopt an already-connected tcp::socket into a server-side
    // ssl::stream and run the server handshake — the read loop arms only on its
    // success (DIVERGENCE from the plaintext accept ctor, which reads at once).
    tls_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected,
                const tls_credential &cred, wire::stream_inbound_config cfg = {})
        : m_io(io)
        , m_ssl_ctx(detail::share_context(cred))
        , m_stream(std::move(connected), m_ssl_ctx)
        , m_inbound(io, cfg)
    {
        wire_inbound();
        m_open = true;
        do_handshake(::asio::ssl::stream_base::server);
    }

    ~tls_channel() { m_inbound.shutdown(); shutdown_socket(); }

    tls_channel(const tls_channel &) = delete;
    tls_channel &operator=(const tls_channel &) = delete;
    tls_channel(tls_channel &&) = delete;
    tls_channel &operator=(tls_channel &&) = delete;

    // Enqueue unconditionally; the drain is gated on handshake completion (the
    // handshake-success callback kicks the first do_write()).
    void send(std::span<const std::byte> data)
    {
        if(!m_open)
            return;
        m_write_queue.emplace_back(data.begin(), data.end());
        if(m_handshake_done && !m_writing)
            do_write();
    }

    void close()
    {
        if(!socket().is_open())
            return;
        m_inbound.shutdown();
        shutdown_socket();
        ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });   // posted, never synchronous
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        std::error_code ec;
        auto ep = socket().remote_endpoint(ec);
        if(ec)
            return {"tls", ""};
        return {"tls", ep.address().to_string() + ":" + std::to_string(ep.port())};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    using lowest_layer_type = ::asio::ssl::stream<::asio::ip::tcp::socket>::lowest_layer_type;
    [[nodiscard]] lowest_layer_type &socket() noexcept { return m_stream.lowest_layer(); }
    [[nodiscard]] const lowest_layer_type &socket() const noexcept { return m_stream.lowest_layer(); }

    // Dial-side bootstrap: set SNI (best-effort) then run the client handshake.
    // The read loop + write drain arm only on handshake success (fail-closed: a
    // verify reject lands in the ec path and fails the channel). on_ready fires
    // ONCE the secure channel is up, so the transport delivers the channel to
    // on_dialed POST-handshake — a verify-rejected peer never yields a live one.
    void start_client_handshake(const std::string &host,
                                plexus::detail::move_only_function<void()> on_ready = {})
    {
        m_open = true;
        m_on_ready = std::move(on_ready);
        if(!host.empty())
            (void)::SSL_set_tlsext_host_name(m_stream.native_handle(), host.c_str());
        do_handshake(::asio::ssl::stream_base::client);
    }

private:
    void wire_inbound()
    {
        m_inbound.on_frame([this](const wire::complete_frame &f) { post_frame(f); });
        m_inbound.on_protocol_close([this](wire::close_cause c) { handle_protocol_close(c); });
    }

    void do_handshake(::asio::ssl::stream_base::handshake_type mode)
    {
        m_stream.async_handshake(mode, [this](std::error_code ec) {
            if(ec)
                return fail(ec);
            m_handshake_done = true;
            do_read();
            if(!m_writing)
                do_write();   // drain whatever queued while the handshake was in flight
            if(m_on_ready)
                m_on_ready();
        });
    }

    void handle_protocol_close(wire::close_cause cause)
    {
        if(m_on_protocol_close)
            m_on_protocol_close(cause);
        close();
    }

    void shutdown_socket()
    {
        auto &sock = socket();
        if(!sock.is_open())
            return;
        std::error_code ec;
        (void)sock.shutdown(::asio::ip::tcp::socket::shutdown_both, ec);
        (void)sock.close(ec);
        m_open = false;
    }

    void do_read()
    {
        m_stream.async_read_some(::asio::buffer(m_read_buf),
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return fail(ec);
                m_inbound.feed(std::span<const std::byte>{m_read_buf.data(), n});
                if(m_open)
                    do_read();
            });
    }

    void post_frame(const wire::complete_frame &frame)
    {
        wire::encode_frame_into(m_frame_scratch, frame.header, frame.payload);
        auto owned = std::make_shared<std::vector<std::byte>>(m_frame_scratch);
        ::asio::post(m_io, [this, owned]
        {
            if(m_on_data)
                m_on_data(std::span<const std::byte>{*owned});
        });
    }

    void do_write()
    {
        if(m_write_queue.empty())
        {
            m_writing = false;
            return;
        }
        m_writing = true;
        ::asio::async_write(m_stream, ::asio::buffer(m_write_queue.front()),
            [this](std::error_code ec, std::size_t)
            {
                m_write_queue.pop_front();
                if(ec)
                    return fail(ec);
                do_write();
            });
    }

    void fail(const std::error_code &ec)
    {
        if(ec == ::asio::error::operation_aborted || !m_open)
            return;
        m_open = false;
        m_writing = false;
        m_inbound.shutdown();
        auto mapped = plexus::asio::detail::map_error(ec);
        if(m_on_error)
            m_on_error(mapped);
        if(m_on_closed)
            m_on_closed();
    }

    ::asio::io_context &m_io;
    ::asio::ssl::context m_ssl_ctx;
    ::asio::ssl::stream<::asio::ip::tcp::socket> m_stream;
    wire::stream_inbound<plexus::asio::asio_timer, ::asio::io_context &> m_inbound;
    std::vector<std::byte> m_frame_scratch;
    std::array<std::byte, 4096> m_read_buf{};
    std::deque<std::vector<std::byte>> m_write_queue;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    plexus::detail::move_only_function<void()> m_on_ready;
    bool m_open{false};
    bool m_writing{false};
    bool m_handshake_done{false};
};

}

static_assert(plexus::io::byte_channel<plexus::tls::tls_channel>,
    "tls_channel must satisfy byte_channel — check the seven verbs");

#endif
