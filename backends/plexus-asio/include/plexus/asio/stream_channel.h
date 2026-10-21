#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/stream_socket_options.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/stream_channel_io.h"
#include "plexus/asio/detail/stream_consumer_hooks.h"
#include "plexus/asio/detail/stream_channel_lifecycle.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/wire_bytes.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/io/detail/close_notification.h"
#include "plexus/stream/detail/send_queue.h"
#include "plexus/detail/compat.h"

#include <asio/write.hpp>
#include <asio/buffer.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <vector>
#include <cstdint>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// A byte_channel over an asio stream type (a bare socket, or an ssl::stream). Each complete
// inbound frame is POSTED to on_data (per the byte_channel contract); a framing violation or a
// no-progress stall raises on_protocol_close — DISTINCT from on_error so the session discriminates
// peer-misbehaved (no re-dial) from network-dropped (re-dial). The Bootstrap expresses the open
// path: plaintext sends reach the egress directly; a TLS bootstrap routes them through an
// open-before-data gate so no plaintext is written before the handshake completes.
template<typename Stream, typename Traits, typename Bootstrap>
class stream_channel : public detail::stream_consumer_hooks
{
    using traits_type = Traits;

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
            , m_ops()
            , m_closed()
            , m_open(false)
    {
        bind_bootstrap();
        detail::stream_wire_hooks(*this);
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
            , m_ops()
            , m_closed()
            , m_open(false)
    {
        bind_bootstrap();
        detail::stream_wire_hooks(*this);
        m_bootstrap.arm_on_accept(*this);
        detail::stream_apply_socket_options(*this);
    }

    ~stream_channel()
    {
        m_ops.on_settled({});
        m_inbound.shutdown();
        detail::stream_shutdown_socket(*this);
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

    // Close abandons the unsent backlog rather than flushing it, so the residual lands on the edge a shed frame does.
    void close()
    {
        detail::stream_close_with(*this, [this] { m_dropped += m_egress.close_and_drain(); });
    }

    io::endpoint remote_endpoint() const
    {
        return Traits::format_endpoint(m_stream);
    }

    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_closed.install(std::move(cb));
        detail::stream_notify_closed(*this);
    }

    decltype(auto) socket() noexcept
    {
        return Traits::lowest_layer(m_stream);
    }
    decltype(auto) socket() const noexcept
    {
        return Traits::lowest_layer(m_stream);
    }
    // Idempotent: the accept path arms the read loop once (arm_on_accept -> start_read_loop); a
    // later transport/session start_read() must re-run mark_open + the socket-option apply but NOT
    // issue a SECOND async_read_some into the shared read buffer (two concurrent reads into one
    // buffer is a data race, and coalesced frames are lost).
    void start_read()
    {
        if(m_ops.closing())
            return;
        m_open = true;
        detail::stream_apply_socket_options(*this);
        if(m_reading)
            return;
        m_reading = true;
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
    std::size_t outstanding_ops() const noexcept
    {
        return m_ops.count();
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
    void enqueue_egress_pair(wire_bytes<> first, wire_bytes<> second)
    {
        if(!m_egress.enqueue_both(std::move(first), std::move(second)))
            detail::stream_on_write_queue_full(*this);
    }
    void start_read_loop()
    {
        if(m_reading || m_ops.closing())
            return;
        m_reading = true;
        detail::stream_do_read(*this);
    }
    void mark_open() noexcept
    {
        m_open = !m_ops.closing();
    }
    Stream &stream() noexcept
    {
        return m_stream;
    }
    void fail(const std::error_code &ec)
    {
        detail::stream_fail(*this, ec);
    }
    void begin_op() noexcept
    {
        m_ops.begin();
    }
    void end_op()
    {
        m_ops.end();
    }

protected:
    Bootstrap &bootstrap() noexcept
    {
        return m_bootstrap;
    }

    // The single read-arm latch for this channel's lifetime. serial_channel reuses it (its own
    // start_read arms serial_do_read), so stream_shutdown_socket's reset clears the guard for both.
    bool m_reading{false};

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
    friend void detail::stream_wire_hooks(Ch &);
    template<typename Ch>
    friend void detail::stream_do_read(Ch &);
    template<typename Ch>
    friend stream::detail::send_queue::send_sink detail::stream_make_send_sink(Ch &);
    template<typename Ch>
    friend void detail::stream_apply_socket_options(Ch &);
    template<typename Ch>
    friend void detail::stream_notify_closed(Ch &);
    template<typename Ch, typename BeforeTeardown>
    friend void detail::stream_close_with(Ch &, BeforeTeardown);
    template<typename Ch>
    friend void detail::stream_shutdown_socket(Ch &);

    void bind_bootstrap()
    {
        if constexpr(requires(Bootstrap &b) { b.bind_drain([](std::span<const std::byte>) {}); })
            m_bootstrap.bind_drain([this](std::span<const std::byte> b) { enqueue_egress(b); });
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
    io::detail::pending_ops m_ops;
    io::detail::close_notification m_closed;
    bool m_open;
};

}

#endif
