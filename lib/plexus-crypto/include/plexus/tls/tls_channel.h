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
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/handshake_gate.h"
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
// ported). The read loop arms only on handshake completion so ciphertext is
// never read as frames; the open-before-data outbound edge is an
// io::detail::handshake_gate that buffers application sends until the secure
// channel is up, then drains them through the serial async_write egress so
// plaintext is never written before the channel is ready. The post-ready serial
// discipline stays a single in-flight async_write (a stream already serializes —
// no serial outbound-queue block is composed here).
class tls_channel
{
public:
    // The bounded congestion=block outbox BYTE budget (allocated at setup, never grown on
    // the hot path): a producer that outruns the encrypted-stream drain back-pressures (or
    // sheds, under drop) at a bounded 16 MiB instead of growing the userspace outbox to the
    // 10+ GB OOM the unbounded deque left possible. Sized at 4x the 4 MiB max-message
    // ceiling — the swept knee (TRANSPORT-POLICY-PROFILE.md §byte-cap sweep), symmetric
    // with asio_channel's plaintext write-queue budget.
    static constexpr std::size_t default_outbox_bytes =
        4u * io::fragmentation_limits::max_message_size;

    // Dial mode: unconnected ssl::stream. The transport async_connects the
    // lowest layer, then calls start_client_handshake(host). The congestion mode + byte
    // budget are the per-channel QoS choice (block = the safe reliable default; drop = the
    // opt-out shed), threaded as required-WITH-default ctor args as udp_channel does.
    tls_channel(::asio::io_context &io, const tls_credential &cred,
                wire::stream_inbound_config cfg = {},
                io::congestion congestion = io::congestion::block,
                std::size_t outbox_bytes = default_outbox_bytes)
        : m_io(io)
        , m_ssl_ctx(detail::share_context(cred))
        , m_stream(io, m_ssl_ctx)
        , m_inbound(io, cfg)
        , m_gate([this](std::span<const std::byte> bytes) { enqueue_egress(bytes); })
        , m_congestion(congestion)
        , m_outbox_bytes(outbox_bytes)
    {
        wire_inbound();
    }

    // Accept mode: adopt an already-connected tcp::socket into a server-side
    // ssl::stream. The server handshake is NOT started here — the listener wires
    // its readiness hook first and then calls start_server_handshake(), so the
    // accepted channel is delivered to the consumer ONLY post-handshake (a
    // verify-rejected peer never yields a live accepted channel — fail-closed,
    // symmetric with the dial side).
    tls_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected,
                const tls_credential &cred, wire::stream_inbound_config cfg = {},
                io::congestion congestion = io::congestion::block,
                std::size_t outbox_bytes = default_outbox_bytes)
        : m_io(io)
        , m_ssl_ctx(detail::share_context(cred))
        , m_stream(std::move(connected), m_ssl_ctx)
        , m_inbound(io, cfg)
        , m_gate([this](std::span<const std::byte> bytes) { enqueue_egress(bytes); })
        , m_congestion(congestion)
        , m_outbox_bytes(outbox_bytes)
    {
        wire_inbound();
    }

    ~tls_channel() { m_inbound.shutdown(); shutdown_socket(); m_gate.reset(); }

    tls_channel(const tls_channel &) = delete;
    tls_channel &operator=(const tls_channel &) = delete;
    tls_channel(tls_channel &&) = delete;
    tls_channel &operator=(tls_channel &&) = delete;

    // Submit to the open-before-data gate: pre-handshake it buffers an owned copy;
    // post-handshake (mark_ready already drained) it forwards straight to the serial
    // async_write egress. The gate's ready edge is the handshake-success callback.
    void send(std::span<const std::byte> data)
    {
        if(!m_open)
            return;
        m_gate.submit(data);
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

    [[nodiscard]] io::congestion congestion_mode() const noexcept { return m_congestion; }
    // The count of frames shed under congestion=drop (the drop-observer's edge).
    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_dropped; }
    // The current queued (un-drained) outbox byte occupancy; 0 when the stream drains.
    [[nodiscard]] std::size_t backpressured() const noexcept { return m_outbox_used; }

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

    // Accept-side bootstrap: run the server handshake. The read loop + write drain
    // arm only on its success; on_ready fires ONCE the secure channel is up, so the
    // listener delivers the accepted channel POST-handshake — a verify-rejected
    // peer never yields a live accepted channel (fail-closed, symmetric with dial).
    void start_server_handshake(plexus::detail::move_only_function<void()> on_ready = {})
    {
        m_open = true;
        m_on_ready = std::move(on_ready);
        do_handshake(::asio::ssl::stream_base::server);
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
            do_read();
            m_gate.mark_ready();   // drain whatever buffered while the handshake was in flight
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

    // The gate's drain sink (post-ready). The gate hands a transient view (an owned
    // node while draining its buffer, or the caller's scratch on the pass-through), so
    // copy it into the owned egress FIFO that keeps the bytes alive across the
    // async_write, then kick the serial drain. This bridge IS the post-ready one-in-
    // flight discipline (a stream already serializes — no outbound-queue block here).
    void enqueue_egress(std::span<const std::byte> bytes)
    {
        if(!admits(bytes.size()))
            return on_outbox_full();
        m_outbox_used += bytes.size();
        m_outbox.emplace_back(bytes.begin(), bytes.end());
        if(!m_writing)
            do_write();
    }

    // Compare-before-add: a frame fits only when the cap is not already met AND its size
    // is within the remaining budget (cap - bytes, no wrap).
    [[nodiscard]] bool admits(std::size_t size) const noexcept
    {
        return m_outbox_used < m_outbox_bytes && size <= m_outbox_bytes - m_outbox_used;
    }

    // congestion=drop sheds the frame at the publisher; congestion=block surfaces
    // would_block (the stall edge — bounded, never unbounded growth).
    void on_outbox_full()
    {
        if(m_congestion == io::congestion::drop)
        {
            ++m_dropped;
            return;
        }
        if(m_on_error)
            m_on_error(io::io_error::would_block);
    }

    void do_write()
    {
        if(m_outbox.empty())
        {
            m_writing = false;
            return;
        }
        m_writing = true;
        ::asio::async_write(m_stream, ::asio::buffer(m_outbox.front()),
            [this](std::error_code ec, std::size_t)
            {
                m_outbox_used -= m_outbox.front().size();
                m_outbox.pop_front();
                if(ec)
                    return fail(ec);
                do_write();
            });
    }

    // A fail edge may fire from inside the channel's own async-completion stack. The
    // channel never destroys itself here: the transport owns the in-flight dial via its
    // pending_dial_registry (copy-before-erase + deferred-destroy), so on_error routes to
    // the transport's fail path, which posts the channel's destruction off this stack.
    // The accept path is owned by the listener's readiness closure (delivered synchronously
    // on the ready edge, no self-owning cycle). fail() only quiesces and reports.
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
    io::detail::handshake_gate m_gate;                 // open-before-data outbound buffer
    io::congestion m_congestion;
    std::size_t m_outbox_bytes;                        // the byte budget (cap)
    std::size_t m_outbox_used{0};                      // summed queued (un-drained) bytes
    std::size_t m_dropped{0};                          // congestion=drop shed count
    std::vector<std::byte> m_frame_scratch;
    std::array<std::byte, 4096> m_read_buf{};
    std::deque<std::vector<std::byte>> m_outbox;       // bounded post-ready async_write FIFO
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    plexus::detail::move_only_function<void()> m_on_ready;
    bool m_open{false};
    bool m_writing{false};
};

}

static_assert(plexus::io::byte_channel<plexus::tls::tls_channel>,
    "tls_channel must satisfy byte_channel — check the seven verbs");

#endif
