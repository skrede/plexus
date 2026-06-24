#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H

// over-limit: one byte_channel core 3-into-1-collapsed for every asio stream backend (TCP,
// AF_UNIX, TLS); the public verbs + the Bootstrap/Traits open seam + the inbound/egress members
// are one cohesive shared core (the Traits/Bootstrap splits keep each per-backend file lean and
// this one the single source of truth), so splitting the surface scatters that shared core — the
// send-queue drive + read-loop glue is extracted to detail/stream_channel_io.h.
//
// One byte_channel core for every asio stream backend (TCP, AF_UNIX, TLS): the bodies below are
// the verbatim core the three hand-maintained channels shared, with the only variation routed
// through a Traits (scheme / endpoint-format / shutdown enum) and a Bootstrap (the open path —
// direct for plaintext, gated-on-handshake for TLS).

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

// The kernel socket knobs a stream backend applies once at the open transition. Every
// field is a required-WITH-default sentinel: 0 (or false) means "leave the kernel default
// untouched", a non-zero value is an explicit consumer-sovereign override applied
// best-effort (the kernel may clamp; a rejection is swallowed since a buffer size is a
// throughput hint, not a correctness requirement). NOT std::optional — absence is not
// meaningful here, the disabled state is a real default. The granular keepalive intervals
// apply ONLY when keepalive is true AND the interval is non-zero (per-platform setsockopt,
// see tcp_traits); on a backend with no TCP keepalive (AF_UNIX) the whole struct is a no-op.
struct stream_socket_options
{
    std::size_t   so_sndbuf               = 0;
    std::size_t   so_rcvbuf               = 0;
    bool          keepalive               = false;
    std::uint32_t keepalive_idle_secs     = 0;
    std::uint32_t keepalive_interval_secs = 0;
    std::uint32_t keepalive_count         = 0;
};

// The per-read kernel buffer default (heap, sized once — never a stack array). 64 KiB clears
// one max TLS record (~16 KiB) and dwarfs the 4 KiB that forced 16-1024 reads per large
// message. It is a required-WITH-default ctor argument on the channel, so a constrained target
// dials it down to ~1-2 KiB; omitting it yields this byte-identical default.
inline constexpr std::size_t k_stream_read_buffer_bytes = 64u * 1024u;

// The fail-closed floor for the read-buffer size. The size is a consumer/QoS config value, not
// an attacker/wire-controlled one; a degenerate (0) request must never produce a silent
// zero-size buffer (a zero-length async_read makes no progress). It is floored to this minimum
// — one TLS record clears with margin — rather than rejected, so a mis-set QoS knob degrades to
// a usable buffer instead of failing construction. There is deliberately no upper clamp here:
// the on-bench footprint ceiling is a separate, later concern.
inline constexpr std::size_t k_min_stream_read_buffer_bytes = 4u * 1024u;

// Resolve a requested read-buffer size to the size actually allocated: a degenerate (sub-floor)
// request floors to k_min_stream_read_buffer_bytes (fail-closed against a zero-size buffer), any
// at-or-above request is honored verbatim.
constexpr std::size_t stream_read_buffer_size(std::size_t requested) noexcept
{
    return requested < k_min_stream_read_buffer_bytes ? k_min_stream_read_buffer_bytes : requested;
}

// A byte_channel over an asio stream type (a bare socket, or an ssl::stream). Inbound bytes
// feed a member stream::stream_inbound (which composes the frame_reassembler and owns the
// no-progress slowloris timer); each complete frame is POSTED to on_data (per the
// byte_channel contract), while a framing violation or a no-progress stall raises
// on_protocol_close — a seam DISTINCT from on_error so the session discriminates
// peer-misbehaved (no re-dial) from network-dropped (re-dial). The channel is caller-owned
// and runs its read loop with `this` captured. The stream::stream_inbound_config (the node's
// hardening config the transport stamps every channel with) is a required-WITH-default ctor
// argument, defaulted to {} so a channel minted with just (io) is a real default-config
// channel, never an unarmed one. The Bootstrap expresses the open path: plaintext sends
// reach the egress directly; a TLS bootstrap routes them through an open-before-data gate so
// no plaintext is written before the handshake completes.
template<typename Stream, typename Traits, typename Bootstrap>
class stream_channel
{
public:
    // Dial/executor-alone: unconnected, not reading yet — the transport arms the open path
    // (start_read for plaintext, the handshake for TLS). BootstrapArgs forward the per-backend
    // construction state (none for plaintext, the credential for TLS) into the Bootstrap. The
    // congestion mode + byte budget are the per-channel QoS choice (block = the safe reliable
    // default that back-pressures; drop_newest = the opt-out shed), threaded as required-
    // WITH-default ctor args exactly as udp_channel threads io::congestion.
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
            , m_egress(detail::stream_make_send_sink(*this), egress.bytes)
    {
        bind_bootstrap();
        detail::stream_wire_inbound(*this);
    }

    // Accept-mode: adopt an already-connected socket. Plaintext starts reading immediately;
    // TLS defers the read loop to its server handshake (arm_on_accept). Connected is the bare
    // socket the Bootstrap wraps into the stream (a tcp::socket for both TCP and TLS, a
    // local-stream socket for AF_UNIX).
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
            , m_egress(detail::stream_make_send_sink(*this), egress.bytes)
    {
        bind_bootstrap();
        detail::stream_wire_inbound(*this);
        m_bootstrap.arm_on_accept(*this);
        apply_socket_options(); // a plaintext accept is already open; a TLS accept defers (is_open
                                // guard)
    }

    // The dtor only tears the stream/timer down — it never posts on_closed (a this-capturing
    // post could outlive the channel). close() posts on_closed.
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

    // Hand the bytes to the Bootstrap's open path: plaintext enqueues straight to the serial
    // async_write egress; TLS submits through its open-before-data gate (buffered pre-
    // handshake, forwarded straight once ready). A full cap sheds (drop_newest) or surfaces
    // would_block (block) — never an unbounded deque.
    void send(std::span<const std::byte> data)
    {
        if(!m_open)
            return;
        m_bootstrap.submit(*this, data);
    }

    // Owner-carry send: hold the supplied wire_bytes owner across the async write so the
    // egress writes its view with NO copy (the plaintext zero-copy path). The Bootstrap
    // routes it: plaintext hands the owner straight to the serial egress; TLS forwards the
    // owner's view through its open-before-data gate (which copies pre-handshake), so the
    // owner overload is byte-equivalent there.
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
        // Count the still-unsent backlog BEFORE teardown and surface it as loss: close abandons
        // the queued bytes rather than flushing them (a synchronous non-blocking write would
        // bypass the TLS layer; a graceful async drain-with-deadline is a separate feature), so
        // the residual is never silently dropped — it lands on the same edge a shed frame does.
        m_dropped += m_egress.close_and_drain();
        shutdown_socket(); // an aborted in-flight write must not chain onto the closed socket
        ::asio::post(m_io,
                     [this]
                     {
                         if(m_on_closed)
                             m_on_closed();
                     }); // posted, never synchronous
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
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

    [[nodiscard]] decltype(auto) socket() noexcept
    {
        return Traits::lowest_layer(m_stream);
    }
    [[nodiscard]] decltype(auto) socket() const noexcept
    {
        return Traits::lowest_layer(m_stream);
    }
    void start_read()
    {
        m_open = true;
        apply_socket_options();
        detail::stream_do_read(*this);
    }

    // The stable per-construction id the egress scheduler keys its band map on (read via a
    // capability probe): unique per object, so a reconnect at a reused heap address cannot
    // bleed a stale band entry across.
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept
    {
        return m_scheduler_key;
    }

    [[nodiscard]] io::congestion congestion_mode() const noexcept
    {
        return m_congestion;
    }
    // The count of frames shed under congestion=drop_newest (the drop-observer's edge).
    [[nodiscard]] std::size_t dropped_count() const noexcept
    {
        return m_dropped;
    }
    // The current queued (un-drained) write-queue byte occupancy; 0 when the socket drains.
    [[nodiscard]] std::size_t backpressured() const noexcept
    {
        return m_egress.queued_bytes();
    }
    // The write-queue byte cap, read by the egress scheduler so its low-water gate tracks THIS
    // channel's actual bound (lockstep): a deepened cap is fed deeper, a shallow one never
    // over-fed.
    [[nodiscard]] std::size_t write_queue_capacity() const noexcept
    {
        return m_egress.capacity();
    }

    // Bootstrap-facing seam (the Bootstrap drives the open path through these): the gate's
    // drain target, the read loop, the open flag, and the stream the handshake runs on.
    void enqueue_egress(std::span<const std::byte> bytes)
    {
        if(!m_egress.enqueue(bytes))
            detail::stream_on_write_queue_full(*this);
    }
    // The owner-carry sibling: hold the supplied owner in the serial egress (no byte copy)
    // across the gather-write completion. Same at-capacity shed/stall edge as the copying
    // overload.
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
    [[nodiscard]] Stream &stream() noexcept
    {
        return m_stream;
    }
    void fail(const std::error_code &ec)
    {
        detail::stream_fail(*this, ec);
    }

protected:
    [[nodiscard]] Bootstrap &bootstrap() noexcept
    {
        return m_bootstrap;
    }

private:
    // The send-queue drive + read-loop glue is relocated to detail/stream_channel_io.h
    // (relocation by friendship): each helper reaches the stream/inbound/egress members below.
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

    // Apply the consumer-supplied kernel knobs once, at the open transition, routing the
    // per-backend divergence through the Traits (tcp applies, unix no-ops). Guarded on an
    // open socket so a not-yet-open TLS accept defers to its own start_read; best-effort
    // (the ec is swallowed — a clamped/rejected size keeps the socket usable).
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

    ::asio::io_context                                                  &m_io;
    Bootstrap                                                            m_bootstrap;
    Stream                                                               m_stream;
    stream::stream_inbound<asio_timer, ::asio::io_context &>             m_inbound;
    std::vector<::asio::const_buffer>                                    m_gather;   // reused gather-write iovec (grows once)
    std::vector<std::byte>                                               m_read_buf; // sized once from the read-buffer ctor param
    io::congestion                                                       m_congestion;
    stream_socket_options                                                m_socket_options;
    std::uint64_t                                                        m_scheduler_key{io::detail::next_scheduler_key()}; // stable per-construction egress key
    std::size_t                                                          m_dropped{0};                                      // congestion=drop shed count
    stream::detail::send_queue                                           m_egress;                                          // bounded byte-budgeted serial write block
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)>               m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)>          m_on_protocol_close;
    bool                                                                 m_open{false};
};

}

#endif
