#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/stream_channel_io.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/wire_bytes.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/stream/detail/send_queue.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/write.hpp>
#include <asio/buffer.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstdint>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// The kernel socket knobs a stream backend applies once at the open transition. Every field is a
// sentinel: 0 (or false) leaves the kernel default untouched, a non-zero value is a best-effort
// override (a rejection is swallowed — a buffer size is a throughput hint). The granular
// keepalive intervals apply ONLY when keepalive is true AND the interval is non-zero.
struct stream_socket_options
{
    std::size_t so_sndbuf                 = 0;
    std::size_t so_rcvbuf                 = 0;
    bool keepalive                        = false;
    std::uint32_t keepalive_idle_secs     = 0;
    std::uint32_t keepalive_interval_secs = 0;
    std::uint32_t keepalive_count         = 0;
};

// 64 KiB clears one max TLS record (~16 KiB) with margin; a constrained target dials it down via
// the channel's read-buffer ctor argument.
inline constexpr std::size_t k_stream_read_buffer_bytes = 64u * 1024u;

// The fail-closed floor: a degenerate (sub-floor) request floors here rather than producing a
// zero-size buffer (a zero-length async_read makes no progress).
inline constexpr std::size_t k_min_stream_read_buffer_bytes = 4u * 1024u;

constexpr std::size_t stream_read_buffer_size(std::size_t requested) noexcept
{
    return requested < k_min_stream_read_buffer_bytes ? k_min_stream_read_buffer_bytes : requested;
}

// A byte_channel over an asio stream type (a bare socket, or an ssl::stream). Each complete
// inbound frame is POSTED to on_data (per the byte_channel contract); a framing violation or a
// no-progress stall raises on_protocol_close — DISTINCT from on_error so the session discriminates
// peer-misbehaved (no re-dial) from network-dropped (re-dial). The Bootstrap expresses the open
// path: plaintext sends reach the egress directly; a TLS bootstrap routes them through an
// open-before-data gate so no plaintext is written before the handshake completes.
template<typename Stream, typename Traits, typename Bootstrap>
class stream_channel
{
public:
    // Dial/executor-alone: unconnected, not reading yet — the transport arms the open path
    // (start_read for plaintext, the handshake for TLS).
    template<typename... BootstrapArgs>
    explicit stream_channel(::asio::io_context &io, stream::stream_inbound_config cfg, io::congestion congestion, io::egress_capacity egress, stream_socket_options socket_options,
                            std::size_t read_buffer_bytes = k_stream_read_buffer_bytes, BootstrapArgs &&...bargs)
            : m_io(io)
            , m_bootstrap(std::forward<BootstrapArgs>(bargs)...)
            , m_stream(m_bootstrap.make_stream(io))
            , m_inbound(io, cfg)
            , m_read_buf(stream_read_buffer_size(read_buffer_bytes))
            , m_congestion(congestion)
            , m_socket_options(socket_options)
            , m_scheduler_key(io::detail::next_scheduler_key())
            , m_dropped(0)
            , m_egress(detail::stream_make_send_sink(*this), egress.bytes)
            , m_open(false)
    {
        bind_bootstrap();
        detail::stream_wire_inbound(*this);
    }

    // Accept-mode: adopt an already-connected socket. Plaintext starts reading immediately; TLS
    // defers the read loop to its server handshake (arm_on_accept).
    template<typename Connected, typename... BootstrapArgs>
    stream_channel(::asio::io_context &io, Connected connected, stream::stream_inbound_config cfg, io::congestion congestion, io::egress_capacity egress,
                   stream_socket_options socket_options, std::size_t read_buffer_bytes = k_stream_read_buffer_bytes, BootstrapArgs &&...bargs)
            : m_io(io)
            , m_bootstrap(std::forward<BootstrapArgs>(bargs)...)
            , m_stream(m_bootstrap.make_stream(io, std::move(connected)))
            , m_inbound(io, cfg)
            , m_read_buf(stream_read_buffer_size(read_buffer_bytes))
            , m_congestion(congestion)
            , m_socket_options(socket_options)
            , m_scheduler_key(io::detail::next_scheduler_key())
            , m_dropped(0)
            , m_egress(detail::stream_make_send_sink(*this), egress.bytes)
            , m_open(false)
    {
        bind_bootstrap();
        detail::stream_wire_inbound(*this);
        m_bootstrap.arm_on_accept(*this);
        apply_socket_options();
    }

    // Never posts on_closed (a this-capturing post could outlive the channel); close() does.
    ~stream_channel()
    {
        m_inbound.shutdown();
        shutdown_socket();
        m_bootstrap.reset();
    }

    stream_channel(const stream_channel &)            = delete;
    stream_channel &operator=(const stream_channel &) = delete;
    stream_channel(stream_channel &&)                 = delete;
    stream_channel &operator=(stream_channel &&)      = delete;

    void send(std::span<const std::byte> data)
    {
        if(!m_open)
            return;
        m_bootstrap.submit(*this, data);
    }

    // Owner-carry: hold the wire_bytes owner across the async write so the egress writes its view
    // with NO copy (the plaintext zero-copy path).
    void send(wire_bytes<> data)
    {
        if(!m_open)
            return;
        m_bootstrap.submit(*this, std::move(data));
    }

    void close()
    {
        if(!socket().is_open())
            return;
        m_inbound.shutdown();
        // Count the still-unsent backlog as loss BEFORE teardown: close abandons the queued bytes
        // rather than flushing them, so the residual lands on the same edge a shed frame does.
        m_dropped += m_egress.close_and_drain();
        // An aborted in-flight write must not chain onto the closed socket.
        shutdown_socket();
        // Posted, never synchronous: a this-capturing on_closed could otherwise run inline.
        ::asio::post(m_io,
                     [this]
                     {
                         if(m_on_closed)
                             m_on_closed();
                     });
    }

    io::endpoint remote_endpoint() const
    {
        return Traits::format_endpoint(m_stream);
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }

    decltype(auto) socket() noexcept
    {
        return Traits::lowest_layer(m_stream);
    }
    decltype(auto) socket() const noexcept
    {
        return Traits::lowest_layer(m_stream);
    }
    void start_read()
    {
        m_open = true;
        apply_socket_options();
        detail::stream_do_read(*this);
    }

    // Unique per object, so a reconnect at a reused heap address cannot bleed a stale band entry
    // into the egress scheduler's band map.
    std::uint64_t scheduler_key() const noexcept
    {
        return m_scheduler_key;
    }

    io::congestion congestion_mode() const noexcept
    {
        return m_congestion;
    }
    std::size_t dropped_count() const noexcept
    {
        return m_dropped;
    }
    std::size_t backpressured() const noexcept
    {
        return m_egress.queued_bytes();
    }
    std::size_t write_queue_capacity() const noexcept
    {
        return m_egress.capacity();
    }

    void enqueue_egress(std::span<const std::byte> bytes)
    {
        if(!m_egress.enqueue(bytes))
            detail::stream_on_write_queue_full(*this);
    }
    void enqueue_egress_owned(wire_bytes<> bytes)
    {
        if(!m_egress.enqueue(std::move(bytes)))
            detail::stream_on_write_queue_full(*this);
    }
    void start_read_loop()
    {
        detail::stream_do_read(*this);
    }
    void mark_open() noexcept
    {
        m_open = true;
    }
    Stream &stream() noexcept
    {
        return m_stream;
    }
    void fail(const std::error_code &ec)
    {
        detail::stream_fail(*this, ec);
    }

protected:
    Bootstrap &bootstrap() noexcept
    {
        return m_bootstrap;
    }

private:
    template<typename Ch>
    friend void detail::stream_on_write_queue_full(Ch &);
    template<typename Ch>
    friend void detail::stream_fail(Ch &, const std::error_code &);
    template<typename Ch>
    friend void detail::stream_handle_protocol_close(Ch &, wire::close_cause);
    template<typename Ch>
    friend void detail::stream_post_frame(Ch &, const wire::complete_frame &);
    template<typename Ch>
    friend void detail::stream_wire_inbound(Ch &);
    template<typename Ch>
    friend void detail::stream_do_read(Ch &);
    template<typename Ch>
    friend stream::detail::send_queue::send_sink detail::stream_make_send_sink(Ch &);

    void bind_bootstrap()
    {
        if constexpr(requires(Bootstrap &b) { b.bind_drain([](std::span<const std::byte>) {}); })
            m_bootstrap.bind_drain([this](std::span<const std::byte> b) { enqueue_egress(b); });
    }

    // Guarded on an open socket so a not-yet-open TLS accept defers to its own start_read;
    // best-effort (the ec is swallowed — a clamped/rejected size keeps the socket usable).
    void apply_socket_options()
    {
        if(!socket().is_open())
            return;
        std::error_code ec;
        Traits::apply_socket_options(socket(), m_socket_options, ec);
    }

    void shutdown_socket()
    {
        auto &sock = socket();
        if(!sock.is_open())
            return;
        std::error_code ec;
        Traits::shutdown(sock, ec);
        (void)sock.close(ec);
        m_open = false;
    }

    ::asio::io_context &m_io;
    Bootstrap m_bootstrap;
    Stream m_stream;
    stream::stream_inbound<asio_timer, ::asio::io_context &> m_inbound;
    std::vector<::asio::const_buffer> m_gather;
    std::vector<std::byte> m_read_buf;
    io::congestion m_congestion;
    stream_socket_options m_socket_options;
    std::uint64_t m_scheduler_key;
    std::size_t m_dropped;
    stream::detail::send_queue m_egress;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    bool m_open;
};

}

#endif
