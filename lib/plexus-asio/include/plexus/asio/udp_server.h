#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/send_queue.h"
#include "plexus/detail/compat.h"

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <utility>
#include <algorithm>
#include <system_error>

namespace plexus::asio {

// The shared bound UDP socket: ONE ::asio::ip::udp::socket per node-port, bound at
// start(), shared across every logical peer. UDP is connectionless — there is no
// per-peer kernel socket, so udp_server owns the only socket and the udp_channel
// facades are stateless views over it. The recv loop reads into a fixed setup-time
// buffer (>= 65536, the UDP datagram max — reused, never per-datagram allocated)
// and hands (sender_endpoint, bytes) to an owner-installed callback, which demuxes.
//
// send_to(span, dest) does NOT reference the caller's bytes across the async op:
// asio buffers are non-owning views and async_send_to does not copy, so a caller-
// owned scratch buffer reused on the next send would transmit corrupted bytes on a
// burst / retransmit-vs-send / multi-peer overlap. The serial outbound discipline
// (copy-into-owned-node + one-in-flight drain) is hoisted into the core send_queue
// block; udp_server holds ONLY the irreducible async_send_to send-sink, so it is a
// thin pump. The block is BYTE-CAPPED at construction: a saturating publisher's overrun
// is refused at the bound (the per-node congestion mode decides — drop_newest sheds,
// block surfaces would_block) instead of growing the shared outbound queue without
// limit, so the process stays memory-bounded under a flood.
//
// Single-owner, bare `this`, no shared_from_this / strand; the owning udp_transport
// closes the socket before the server dies, so a completion firing after teardown
// self-guards on operation_aborted.
class udp_server
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    // The bounded outbound-queue BYTE budget (allocated at setup, never grown on the hot
    // path): a saturating publisher's overrun is refused at the bound, so the shared queue
    // never grows without limit. The budget is FLOORED at one per-message ceiling because a
    // single oversize message fragments into N synchronously-enqueued datagrams: a cap
    // sized for one datagram refuses fragment 1 once fragment 0 fills it, and best-effort
    // drops the refused fragment forever (the message never reassembles) while reliable
    // throttles its window to one segment in flight. The floor admits one whole message's
    // fragment burst — exactly the rule udp_channel applies to its per-channel back-pressure
    // queue — keeping the cap O(one message), DoS-safe, never unbounded. A load-bearing
    // knob, to be substantiated at the fan-out benchmark.
    static constexpr std::size_t default_send_queue_bytes =
        std::max<std::size_t>(65536, io::global_default_max_message_bytes);

    // The kernel socket send/receive buffer sizes (SO_SNDBUF/SO_RCVBUF) applied to the one
    // bound socket. The sentinel 0 means "leave the kernel default untouched" — the
    // empirically-substantiated default, since a small-datagram loopback sweep over
    // {default, 256 KiB, 1 MiB, 4 MiB} moved the p50 by less than the 2% noise bar (the
    // hot path never queues in the kernel buffer at one-datagram-in-flight). A non-zero
    // value is an explicit consumer-sovereign override (never silent): it is applied once
    // at start(), after open() and before bind(), via setsockopt on the native handle.
    static constexpr std::size_t default_so_sndbuf = 0;
    static constexpr std::size_t default_so_rcvbuf = 0;

    // The congestion mode is the per-node QoS choice (block = the safe reliable default
    // that surfaces would_block at the cap; drop_newest = the opt-out shed), threaded with
    // the byte budget as required-WITH-default ctor args exactly as the stream channels.
    // The socket-buffer sizes are required-with-default the same way (0 = kernel default).
    explicit udp_server(::asio::io_context &io, io::congestion congestion = io::congestion::block,
                        std::size_t send_queue_bytes = default_send_queue_bytes,
                        std::size_t so_sndbuf = default_so_sndbuf,
                        std::size_t so_rcvbuf = default_so_rcvbuf)
        : m_socket(io)
        , m_congestion(congestion)
        , m_so_sndbuf(so_sndbuf)
        , m_so_rcvbuf(so_rcvbuf)
        , m_send_queue(make_send_sink(), send_queue_bytes)
    {
    }

    udp_server(const udp_server &) = delete;
    udp_server &operator=(const udp_server &) = delete;

    ~udp_server() { close(); }

    // Bind the one socket to bind_ep and arm the single receive loop. Fail-closed:
    // a bind error is reported through on_error and the loop is never armed.
    void start(const endpoint_type &bind_ep)
    {
        std::error_code ec;
        m_socket.open(bind_ep.protocol(), ec);
        if(ec)
            return report(ec);
        // The idle fast-path issues a synchronous send_to on the io thread; non-blocking
        // mode makes a full socket buffer return would_block (the enqueue fallback) instead
        // of stalling the thread. Best-effort like the buffer options: asio's async recv/send
        // reactor manages descriptor readiness itself, so this is compatible with the loop.
        (void)m_socket.non_blocking(true, ec);
        apply_socket_buffers();
        m_socket.bind(bind_ep, ec);
        if(ec)
            return report(ec);
        m_open = true;
        do_receive();
    }

    // Hand the caller's bytes to the byte-capped send_queue block, which copies them into
    // an owned node (synchronously, so the caller's scratch is never referenced live across
    // the async op) and drives the serial drain through the async_send_to send-sink. A
    // datagram that would carry the queued total past the cap is refused: the per-node
    // congestion mode then decides — drop_newest sheds it (counted), block surfaces
    // would_block (the stall edge) — never an unbounded queue. Compare-before-add (no wrap).
    void send_to(std::span<const std::byte> bytes, const endpoint_type &dest)
    {
        if(!m_open)
            return;
        if(!m_send_queue.enqueue(bytes, dest))
            on_send_queue_full();
    }

    // Send a STANDALONE single datagram (a whole small message, never one of a fragmented
    // burst): when the outbound queue is idle a SYNCHRONOUS send_to hands the datagram to the
    // kernel during the call (the kernel copies it out), so the caller's scratch is free on
    // return — no owned node, no async round-trip, no per-datagram copy (the small-datagram
    // latency win). It fires ONLY when idle, so a sync send can never reorder ahead of
    // queued/in-flight bytes; a would_block falls through to the byte-capped async queue,
    // after which subsequent sends enqueue in order until it drains. The sync result mirrors
    // the async completion's error discrimination. NOT for a fragment burst (use send_to):
    // an unpaced synchronous blast of a multi-datagram best-effort message overruns the
    // receiver — every fragment drops with no retransmit, losing the whole message — whereas
    // the queue's serial async drain interleaves with the receiver on the io_context.
    void send_standalone_to(std::span<const std::byte> bytes, const endpoint_type &dest)
    {
        if(!m_open)
            return;
        if(m_send_queue.queued_bytes() == 0 && !m_send_queue.sending())
        {
            std::error_code ec;
            (void)m_socket.send_to(::asio::buffer(bytes.data(), bytes.size()), dest, 0, ec);
            if(!ec)
                return;
            if(ec != ::asio::error::would_block)
                return on_sync_send_error(ec);
        }
        if(!m_send_queue.enqueue(bytes, dest))
            on_send_queue_full();
    }

    // Installed by the transport: (sender, datagram bytes) per inbound completion.
    void on_datagram(plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram = std::move(cb);
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    [[nodiscard]] std::uint16_t port() const
    {
        std::error_code ec;
        return m_socket.local_endpoint(ec).port();
    }

    [[nodiscard]] bool is_open() const noexcept { return m_open; }

    // The current queued (un-drained) outbound byte occupancy; 0 when the socket drains.
    [[nodiscard]] std::size_t queued_send_bytes() const noexcept { return m_send_queue.queued_bytes(); }

    [[nodiscard]] io::congestion congestion_mode() const noexcept { return m_congestion; }
    // The configured SO_SNDBUF/SO_RCVBUF override (0 = kernel default left untouched).
    [[nodiscard]] std::size_t so_sndbuf() const noexcept { return m_so_sndbuf; }
    [[nodiscard]] std::size_t so_rcvbuf() const noexcept { return m_so_rcvbuf; }
    // The count of datagrams shed under congestion=drop_newest at the byte cap.
    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_dropped; }
    // The count of datagrams discarded on a transient per-datagram send error (an
    // unreachable destination, a momentary buffer-full) — best-effort UDP loss that is NOT
    // surfaced through on_error per datagram, so an unreachable port cannot storm the owner.
    [[nodiscard]] std::size_t send_error_count() const noexcept { return m_send_errors; }
    // The count of spurious connection_reset signals swallowed on the recv path (a Windows
    // ICMP port-unreachable surfacing as a reset on the next recv) — re-armed silently, NOT
    // surfaced through on_error, so an unreachable peer cannot storm the owner.
    [[nodiscard]] std::size_t recv_reset_count() const noexcept { return m_recv_resets; }

    void close()
    {
        if(!m_socket.is_open())
            return;
        std::error_code ec;
        (void)m_socket.close(ec);
        m_open = false;
        m_send_queue.close();
    }

private:
    // The shared-queue congestion safety net at the byte cap: drop_newest sheds the
    // overrun datagram at the publisher (counted), block surfaces would_block (the stall
    // edge) — mirroring the stream channel's on_write_queue_full. Either way the call
    // returns without blocking and the queue never grows past the cap.
    void on_send_queue_full()
    {
        if(m_congestion == io::congestion::drop_newest)
        {
            ++m_dropped;
            return;
        }
        if(m_on_error)
            m_on_error(io::io_error::would_block);
    }

    // The irreducible asio send-sink the send_queue block drives: send the block-owned
    // node's bytes and signal completion when the async op finishes. At most one is
    // outstanding (the block's serial discipline), so a node's bytes stay valid until its
    // own completion runs. The completion discriminates the error class (mirroring how the
    // stream channels split fatal from teardown): a transient per-datagram failure (an
    // unreachable destination, a momentary buffer-full) is best-effort UDP loss — counted,
    // NOT surfaced through on_error per datagram, and the drain chains past it; a fatal
    // socket error closes the queue (stopping the chain so the same failure does not loop)
    // and is reported once; an aborted/post-teardown completion is a guarded no-op.
    io::detail::send_queue<endpoint_type>::send_sink make_send_sink()
    {
        return [this](std::span<const std::byte> bytes, const endpoint_type &dest,
                      io::detail::send_queue<endpoint_type>::completion done)
        {
            m_socket.async_send_to(::asio::buffer(bytes.data(), bytes.size()), dest,
                [this, done = std::move(done)](std::error_code ec, std::size_t) mutable
                {
                    if(ec == ::asio::error::operation_aborted || !m_open)
                        return;
                    if(!ec)
                        return done(true);
                    if(transient_send_error(ec))
                    {
                        ++m_send_errors;   // best-effort UDP loss: chain past it, do not storm on_error
                        return done(true);
                    }
                    report(ec);            // a fatal socket error: report once and stop the chain
                    m_send_queue.close();
                });
        };
    }

    // The idle fast-path's synchronous send hit a non-would_block error: discriminate it
    // exactly as the async completion does. A transient per-datagram failure (an unreachable
    // destination, a momentary buffer-full not surfaced as would_block) is best-effort UDP
    // loss — counted, NOT surfaced through on_error per datagram; a fatal socket error is
    // reported once and closes the queue (so the same failure does not loop on the next send).
    void on_sync_send_error(const std::error_code &ec)
    {
        if(transient_send_error(ec))
        {
            ++m_send_errors;
            return;
        }
        report(ec);
        m_send_queue.close();
    }

    // A transient send error is a per-datagram, best-effort UDP failure (an unreachable
    // destination via an ICMP error, a momentary buffer-full) that the next datagram may
    // not hit — discarded silently, the drain continues. A fatal error indicates the socket
    // itself is unusable going forward (a bad descriptor, an unsupported op, the network
    // down), where chaining would only loop the same failure; everything not in that fatal
    // set is treated as transient so best-effort delivery is the default.
    static bool transient_send_error(const std::error_code &ec) noexcept
    {
        return ec != ::asio::error::bad_descriptor
               && ec != ::asio::error::operation_not_supported
               && ec != ::asio::error::network_down;
    }

    // Apply the consumer-supplied socket-buffer overrides once, between open() and bind().
    // The portable asio options carry to macOS/Linux/Windows; a 0 override leaves the kernel
    // default. A setsockopt rejection is non-fatal best-effort — the kernel may clamp or
    // ignore a size and the socket stays usable, so the error is swallowed (the sizes are a
    // throughput hint, not a correctness requirement).
    void apply_socket_buffers()
    {
        std::error_code ec;
        if(m_so_sndbuf != 0)
            (void)m_socket.set_option(::asio::socket_base::send_buffer_size(static_cast<int>(m_so_sndbuf)), ec);
        if(m_so_rcvbuf != 0)
            (void)m_socket.set_option(::asio::socket_base::receive_buffer_size(static_cast<int>(m_so_rcvbuf)), ec);
    }

    void do_receive()
    {
        m_socket.async_receive_from(::asio::buffer(m_recv_buf), m_sender,
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return fail(ec);
                if(m_on_datagram)
                    m_on_datagram(m_sender, std::span<const std::byte>{m_recv_buf.data(), n});
                if(m_open)
                    do_receive();
            });
    }

    void fail(const std::error_code &ec)
    {
        if(ec == ::asio::error::operation_aborted || !m_open)
            return;
        if(ec == ::asio::error::connection_reset)
        {
            // On Windows a prior send to an unreachable port surfaces the ICMP
            // port-unreachable as connection_reset on the NEXT recv; on POSIX it rarely
            // fires. It is a spurious per-datagram signal on a connectionless socket, NOT a
            // socket fault — re-arm silently so an unreachable peer cannot storm on_error
            // (mirrors the transient-send discrimination). Counted for observability only.
            ++m_recv_resets;
            if(m_open)
                do_receive();
            return;
        }
        report(ec);                 // a transient recv error is surfaced; the loop re-arms
        if(m_open)
            do_receive();
    }

    void report(const std::error_code &ec)
    {
        if(m_on_error)
            m_on_error(detail::map_error(ec));
    }

    ::asio::ip::udp::socket m_socket;
    endpoint_type m_sender{};
    std::array<std::byte, 65536> m_recv_buf{};
    io::congestion m_congestion{io::congestion::block};
    std::size_t m_so_sndbuf{0};                           // SO_SNDBUF override; 0 = kernel default
    std::size_t m_so_rcvbuf{0};                           // SO_RCVBUF override; 0 = kernel default
    std::size_t m_dropped{0};                             // congestion=drop shed count
    std::size_t m_send_errors{0};                         // transient per-datagram send-failure discards
    std::size_t m_recv_resets{0};                         // spurious recv connection_reset signals swallowed
    io::detail::send_queue<endpoint_type> m_send_queue;   // byte-capped owned outbound discipline
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_open{false};
};

}

#endif
