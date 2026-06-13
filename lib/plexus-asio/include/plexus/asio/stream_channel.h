#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_CHANNEL_H

// One byte_channel core for every asio stream backend (TCP, AF_UNIX, TLS): the bodies
// below are the verbatim core the three hand-maintained channels shared, with the only
// variation routed through a Traits (scheme / endpoint-format / shutdown enum) and a
// Bootstrap (the open path — direct for plaintext, gated-on-handshake for TLS). This file
// exceeds the 200-LOC guideline because it is a 3-into-1 collapse; the Traits/Bootstrap
// splits keep every per-backend file lean and this one the single source of truth.

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/wire_bytes.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/stream_send_queue.h"
#include "plexus/io/detail/scheduler_key.h"
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
    std::size_t   so_sndbuf = 0;
    std::size_t   so_rcvbuf = 0;
    bool          keepalive = false;
    std::uint32_t keepalive_idle_secs = 0;
    std::uint32_t keepalive_interval_secs = 0;
    std::uint32_t keepalive_count = 0;
};

// A byte_channel over an asio stream type (a bare socket, or an ssl::stream). Inbound bytes
// feed a member wire::stream_inbound (which composes the frame_reassembler and owns the
// no-progress slowloris timer); each complete frame is POSTED to on_data (per the
// byte_channel contract), while a framing violation or a no-progress stall raises
// on_protocol_close — a seam DISTINCT from on_error so the session discriminates
// peer-misbehaved (no re-dial) from network-dropped (re-dial). The channel is caller-owned
// and runs its read loop with `this` captured. The wire::stream_inbound_config (the node's
// hardening config the transport stamps every channel with) is a required-WITH-default ctor
// argument, defaulted to {} so a channel minted with just (io) is a real default-config
// channel, never an unarmed one. The Bootstrap expresses the open path: plaintext sends
// reach the egress directly; a TLS bootstrap routes them through an open-before-data gate so
// no plaintext is written before the handshake completes.
template <typename Stream, typename Traits, typename Bootstrap>
class stream_channel
{
public:
    // The bounded congestion=block write-queue BYTE budget (allocated at setup, never grown
    // on the hot path): a producer that outruns the socket drain back-pressures (or sheds,
    // under drop_newest) at a bounded cap instead of growing the userspace write deque to the
    // OOM the unbounded queue left possible. Sized at 1x the 4 MiB max-message ceiling so this
    // per-connection FIFO is the SHALLOW socket-facing buffer holding roughly one frame in
    // flight: the deep, priority-ordered backlog lives in the forwarder's egress bands ABOVE
    // this queue, so a deep channel FIFO would silently defeat banding by re-accumulating an
    // un-prioritized backlog. A load-bearing knob — to be substantiated at the fan-out
    // benchmark, not fixed by feel.
    static constexpr std::size_t default_write_queue_bytes =
        1u * io::fragmentation_limits::max_message_size;

    // The per-read kernel buffer (heap, sized once — never a stack array). 64 KiB clears one max
    // TLS record (~16 KiB) and dwarfs the 4 KiB that forced 16-1024 reads per large message.
    static constexpr std::size_t k_stream_read_buffer_bytes = 64u * 1024u;

    // Dial/executor-alone: unconnected, not reading yet — the transport arms the open path
    // (start_read for plaintext, the handshake for TLS). BootstrapArgs forward the per-backend
    // construction state (none for plaintext, the credential for TLS) into the Bootstrap. The
    // congestion mode + byte budget are the per-channel QoS choice (block = the safe reliable
    // default that back-pressures; drop_newest = the opt-out shed), threaded as required-
    // WITH-default ctor args exactly as udp_channel threads io::congestion.
    template <typename... BootstrapArgs>
    explicit stream_channel(::asio::io_context &io, wire::stream_inbound_config cfg,
                            io::congestion congestion, std::size_t write_queue_bytes,
                            stream_socket_options socket_options, BootstrapArgs &&...bargs)
        : m_io(io)
        , m_bootstrap(std::forward<BootstrapArgs>(bargs)...)
        , m_stream(m_bootstrap.make_stream(io))
        , m_inbound(io, cfg)
        , m_congestion(congestion)
        , m_socket_options(socket_options)
        , m_egress(make_send_sink(), write_queue_bytes)
    {
        bind_bootstrap();
        wire_inbound();
    }

    // Accept-mode: adopt an already-connected socket. Plaintext starts reading immediately;
    // TLS defers the read loop to its server handshake (arm_on_accept). Connected is the bare
    // socket the Bootstrap wraps into the stream (a tcp::socket for both TCP and TLS, a
    // local-stream socket for AF_UNIX).
    template <typename Connected, typename... BootstrapArgs>
    stream_channel(::asio::io_context &io, Connected connected, wire::stream_inbound_config cfg,
                   io::congestion congestion, std::size_t write_queue_bytes,
                   stream_socket_options socket_options, BootstrapArgs &&...bargs)
        : m_io(io)
        , m_bootstrap(std::forward<BootstrapArgs>(bargs)...)
        , m_stream(m_bootstrap.make_stream(io, std::move(connected)))
        , m_inbound(io, cfg)
        , m_congestion(congestion)
        , m_socket_options(socket_options)
        , m_egress(make_send_sink(), write_queue_bytes)
    {
        bind_bootstrap();
        wire_inbound();
        m_bootstrap.arm_on_accept(*this);
        apply_socket_options();   // a plaintext accept is already open; a TLS accept defers (is_open guard)
    }

    // The dtor only tears the stream/timer down — it never posts on_closed (a this-capturing
    // post could outlive the channel). close() posts on_closed.
    ~stream_channel() { m_inbound.shutdown(); shutdown_socket(); m_bootstrap.reset(); }

    stream_channel(const stream_channel &) = delete;
    stream_channel &operator=(const stream_channel &) = delete;
    stream_channel(stream_channel &&) = delete;
    stream_channel &operator=(stream_channel &&) = delete;

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
        shutdown_socket();   // an aborted in-flight write must not chain onto the closed socket
        ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });   // posted, never synchronous
    }

    [[nodiscard]] io::endpoint remote_endpoint() const { return Traits::format_endpoint(m_stream); }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    [[nodiscard]] decltype(auto) socket() noexcept { return Traits::lowest_layer(m_stream); }
    [[nodiscard]] decltype(auto) socket() const noexcept { return Traits::lowest_layer(m_stream); }
    void start_read() { m_open = true; apply_socket_options(); do_read(); }

    // The stable per-construction id the egress scheduler keys its band map on (read via a
    // capability probe): unique per object, so a reconnect at a reused heap address cannot
    // bleed a stale band entry across.
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept { return m_scheduler_key; }

    [[nodiscard]] io::congestion congestion_mode() const noexcept { return m_congestion; }
    // The count of frames shed under congestion=drop_newest (the drop-observer's edge).
    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_dropped; }
    // The current queued (un-drained) write-queue byte occupancy; 0 when the socket drains.
    [[nodiscard]] std::size_t backpressured() const noexcept { return m_egress.queued_bytes(); }

    // Bootstrap-facing seam (the Bootstrap drives the open path through these): the gate's
    // drain target, the read loop, the open flag, and the stream the handshake runs on.
    void enqueue_egress(std::span<const std::byte> bytes)
    {
        if(!m_egress.enqueue(bytes))
            on_write_queue_full();
    }
    // The owner-carry sibling: hold the supplied owner in the serial egress (no byte copy)
    // across the gather-write completion. Same at-capacity shed/stall edge as the copying
    // overload.
    void enqueue_egress_owned(wire_bytes<> bytes)
    {
        if(!m_egress.enqueue(std::move(bytes)))
            on_write_queue_full();
    }
    void start_read_loop() { do_read(); }
    void mark_open() noexcept { m_open = true; }
    [[nodiscard]] Stream &stream() noexcept { return m_stream; }
    void fail(const std::error_code &ec) { fail_impl(ec); }

protected:
    [[nodiscard]] Bootstrap &bootstrap() noexcept { return m_bootstrap; }

private:
    void bind_bootstrap()
    {
        if constexpr(requires(Bootstrap &b) { b.bind_drain([](std::span<const std::byte>) {}); })
            m_bootstrap.bind_drain([this](std::span<const std::byte> b) { enqueue_egress(b); });
    }

    // The per-connection congestion safety net: it guards the DIRECT-send bypass paths (latch
    // replay, control frames, fetch_latched) and a misconfigured low-water gate, at a
    // granularity distinct from the forwarder's per-band overflow (no double-application).
    // congestion=drop_newest sheds the frame at the publisher (the documented opt-out of the
    // reliable guarantee); congestion=block surfaces would_block (the stall edge — bounded,
    // never unbounded growth). Either way the call returns without blocking.
    void on_write_queue_full()
    {
        if(m_congestion == io::congestion::drop_newest)
        {
            ++m_dropped;
            return;
        }
        if(m_on_error)
            m_on_error(io::io_error::would_block);
    }

    void wire_inbound()
    {
        m_inbound.on_frame([this](const wire::complete_frame &f) { post_frame(f); });
        m_inbound.on_protocol_close([this](wire::close_cause c) { handle_protocol_close(c); });
    }

    // A peer that misbehaved on the wire: fire the distinct protocol-close seam, then tear
    // down via the on_closed-only close() path — NEVER fail()/on_error.
    void handle_protocol_close(wire::close_cause cause)
    {
        if(m_on_protocol_close)
            m_on_protocol_close(cause);
        close();
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

    void do_read()
    {
        m_stream.async_read_some(::asio::buffer(m_read_buf),
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return fail_impl(ec);
                m_inbound.feed(std::span<const std::byte>{m_read_buf.data(), n});
                if(m_open)   // a protocol-close may have torn the socket down
                    do_read();
            });
    }

    // The stream_inbound on_frame target: the reassembler already materialized the full
    // header-on frame as the owning shared_bytes, so on_data delivers it verbatim. The posted
    // closure CAPTURES the owner, keeping the bytes alive past this call (the post runs later).
    void post_frame(const wire::complete_frame &frame)
    {
        wire_bytes<> owned{frame.payload};
        ::asio::post(m_io, [this, owned = std::move(owned)]
        {
            if(m_on_data)
                m_on_data(static_cast<std::span<const std::byte>>(owned));
        });
    }

    // The irreducible asio send-sink the stream_send_queue block drives: gather the block-
    // owned node views into one ConstBufferSequence and issue a SINGLE async_write (asio lowers
    // the sequence to one writev/WSASend — N frames, one syscall; for TLS, OpenSSL coalesces
    // the gathered plaintext into fewer records around its one AEAD seal) and signal completion
    // when it finishes. At most one is outstanding (the block's serial discipline). On a socket
    // error the channel fails (which closes the block), so the completion's open-guard stops the
    // chain — the exact fail-before-chain edge the hand-rolled do_write carried, never a swallow.
    io::detail::stream_send_queue::send_sink make_send_sink()
    {
        return [this](io::detail::stream_send_queue::buffer_sequence views,
                      io::detail::stream_send_queue::completion done)
        {
            m_gather.clear();
            m_gather.reserve(views.size());
            for(const auto &v : views)
                m_gather.emplace_back(v.data(), v.size());
            ::asio::async_write(m_stream, m_gather,
                [this, done = std::move(done)](std::error_code ec, std::size_t) mutable
                {
                    if(ec)
                        fail_impl(ec);
                    done(!ec);
                });
        };
    }

    void fail_impl(const std::error_code &ec)
    {
        if(ec == ::asio::error::operation_aborted || !m_open)
            return;
        m_open = false;
        m_egress.close();   // stop the serial drain so the failed write does not chain
        m_inbound.shutdown();
        auto mapped = detail::map_error(ec);
        if(m_on_error)
            m_on_error(mapped);
        if(m_on_closed)
            m_on_closed();
    }

    ::asio::io_context &m_io;
    Bootstrap m_bootstrap;
    Stream m_stream;
    wire::stream_inbound<asio_timer, ::asio::io_context &> m_inbound;
    std::vector<::asio::const_buffer> m_gather;           // reused gather-write iovec (grows once)
    std::vector<std::byte> m_read_buf = std::vector<std::byte>(k_stream_read_buffer_bytes);
    io::congestion m_congestion;
    stream_socket_options m_socket_options;
    std::uint64_t m_scheduler_key{io::detail::next_scheduler_key()};   // stable per-construction egress key
    std::size_t m_dropped{0};                             // congestion=drop shed count
    io::detail::stream_send_queue m_egress;               // bounded byte-budgeted serial write block
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    bool m_open{false};
};

}

#endif
