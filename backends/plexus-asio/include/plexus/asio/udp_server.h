#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H

#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/udp_server_dispatch.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/datagram/detail/send_queue.h"
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

// over-limit: one cohesive shared-socket byte pump; the send/recv verbs + the byte-capped
// outbound queue + the recv buffer + the config the ctor binds are one whole, so splitting the
// public face scatters that shared socket state — the bind/per-datagram dispatch glue is
// extracted to detail/udp_server_dispatch.h.
//
// The shared bound UDP socket: ONE ::asio::ip::udp::socket per node-port, bound at start(),
// shared across every logical peer (UDP is connectionless — no per-peer kernel socket). The recv
// loop reads into a fixed setup-time buffer and hands (sender, bytes) to an owner-installed
// callback. The serial outbound discipline is hoisted into the core send_queue block (BYTE-CAPPED
// at construction); udp_server holds ONLY the irreducible async_send_to send-sink — a thin pump.
// Single-owner, bare `this`, no shared_from_this / strand; the owning udp_transport closes the
// socket before the server dies, so a completion after teardown self-guards on operation_aborted.
class udp_server
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    // The bounded outbound-queue BYTE budget (allocated at setup, never grown on the hot path):
    // a saturating publisher's overrun is refused at the bound. FLOORED at one per-message
    // ceiling so a single oversize message's fragment burst is admitted whole (a one-datagram cap
    // would refuse fragment 1 and lose the message). O(one message), DoS-safe, never unbounded.
    static constexpr std::size_t default_send_queue_bytes = std::max<std::size_t>(65536, io::global_default_max_message_bytes);

    // The kernel SO_SNDBUF/SO_RCVBUF overrides; 0 = leave the kernel default (the
    // empirically-substantiated default — a loopback sweep moved p50 under the 2% noise bar). A
    // non-zero value is a consumer-sovereign override applied once at start() (after open, before
    // bind).
    static constexpr std::size_t default_so_sndbuf = 0;
    static constexpr std::size_t default_so_rcvbuf = 0;

    // The congestion mode is the per-node QoS choice (block surfaces would_block at the cap;
    // drop_newest sheds), threaded with the byte budget + socket-buffer sizes as
    // required-with-default ctor args.
    explicit udp_server(::asio::io_context &io, io::congestion congestion = io::congestion::block, std::size_t send_queue_bytes = default_send_queue_bytes,
                        std::size_t so_sndbuf = default_so_sndbuf, std::size_t so_rcvbuf = default_so_rcvbuf)
            : m_socket(io)
            , m_congestion(congestion)
            , m_so_sndbuf(so_sndbuf)
            , m_so_rcvbuf(so_rcvbuf)
            , m_send_queue(detail::make_send_sink(*this), send_queue_bytes)
    {
    }

    udp_server(const udp_server &)            = delete;
    udp_server &operator=(const udp_server &) = delete;

    ~udp_server()
    {
        close();
    }

    // Bind the one socket to bind_ep and arm the single receive loop. Fail-closed:
    // a bind error is reported through on_error and the loop is never armed.
    void start(const endpoint_type &bind_ep)
    {
        std::error_code ec;
        m_socket.open(bind_ep.protocol(), ec);
        if(ec)
            return detail::server_report(*this, ec);
        // The idle fast-path issues a synchronous send_to on the io thread; non-blocking mode
        // makes a full socket buffer return would_block (the enqueue fallback) instead of
        // stalling the thread. asio's reactor manages descriptor readiness, so this is compatible.
        (void)m_socket.non_blocking(true, ec);
        detail::apply_socket_buffers(*this);
        m_socket.bind(bind_ep, ec);
        if(ec)
            return detail::server_report(*this, ec);
        m_open = true;
        detail::do_receive(*this);
    }

    // Hand the caller's bytes to the byte-capped send_queue block (it copies them into an owned
    // node synchronously, so the caller's scratch is never referenced live across the async op).
    // A datagram past the cap is refused — the per-node congestion mode then sheds or surfaces
    // would_block, never an unbounded queue.
    void send_to(std::span<const std::byte> bytes, const endpoint_type &dest)
    {
        if(!m_open)
            return;
        if(!m_send_queue.enqueue(bytes, dest))
            detail::on_send_queue_full(*this);
    }

    // Send a STANDALONE single datagram (a whole small message): when the outbound queue is idle a
    // SYNCHRONOUS send_to hands the datagram to the kernel during the call (no owned node, no async
    // round-trip — the small-datagram latency win). It fires ONLY when idle so it can never reorder
    // ahead of queued bytes; a would_block falls through to the byte-capped async queue. NOT for a
    // fragment burst (use send_to): an unpaced synchronous blast overruns the receiver and loses
    // the whole best-effort message — the queue's serial async drain interleaves on the io_context.
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
                return detail::on_sync_send_error(*this, ec);
        }
        if(!m_send_queue.enqueue(bytes, dest))
            detail::on_send_queue_full(*this);
    }

    // Installed by the transport: (sender, datagram bytes) per inbound completion.
    void on_datagram(plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram = std::move(cb);
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    [[nodiscard]] std::uint16_t port() const
    {
        std::error_code ec;
        return m_socket.local_endpoint(ec).port();
    }

    [[nodiscard]] bool is_open() const noexcept
    {
        return m_open;
    }

    // The current queued (un-drained) outbound byte occupancy; 0 when the socket drains.
    [[nodiscard]] std::size_t queued_send_bytes() const noexcept
    {
        return m_send_queue.queued_bytes();
    }

    [[nodiscard]] io::congestion congestion_mode() const noexcept
    {
        return m_congestion;
    }
    // The configured SO_SNDBUF/SO_RCVBUF override (0 = kernel default left untouched).
    [[nodiscard]] std::size_t so_sndbuf() const noexcept
    {
        return m_so_sndbuf;
    }
    [[nodiscard]] std::size_t so_rcvbuf() const noexcept
    {
        return m_so_rcvbuf;
    }
    // The count of datagrams shed under congestion=drop_newest at the byte cap.
    [[nodiscard]] std::size_t dropped_count() const noexcept
    {
        return m_dropped;
    }
    // The count of datagrams discarded on a transient per-datagram send error (an
    // unreachable destination, a momentary buffer-full) — best-effort UDP loss that is NOT
    // surfaced through on_error per datagram, so an unreachable port cannot storm the owner.
    [[nodiscard]] std::size_t send_error_count() const noexcept
    {
        return m_send_errors;
    }
    // The count of spurious connection_reset signals swallowed on the recv path (a Windows
    // ICMP port-unreachable surfacing as a reset on the next recv) — re-armed silently, NOT
    // surfaced through on_error, so an unreachable peer cannot storm the owner.
    [[nodiscard]] std::size_t recv_reset_count() const noexcept
    {
        return m_recv_resets;
    }

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
    // The bind / per-datagram dispatch glue is relocated to detail/udp_server_dispatch.h
    // (relocation by friendship): each helper reaches the socket/send-queue/sink members below.
    template<typename S>
    friend void detail::server_report(S &, const std::error_code &);
    template<typename S>
    friend void detail::on_send_queue_full(S &);
    template<typename S>
    friend void detail::on_sync_send_error(S &, const std::error_code &);
    template<typename S>
    friend auto detail::make_send_sink(S &) -> typename datagram::detail::send_queue<typename S::endpoint_type>::send_sink;
    template<typename S>
    friend void detail::apply_socket_buffers(S &);
    template<typename S>
    friend void detail::fail(S &, const std::error_code &);
    template<typename S>
    friend void detail::do_receive(S &);

    ::asio::ip::udp::socket                                                                     m_socket;
    endpoint_type                                                                               m_sender{};
    std::array<std::byte, 65536>                                                                m_recv_buf{};
    io::congestion                                                                              m_congestion{io::congestion::block};
    std::size_t                                                                                 m_so_sndbuf{0};   // SO_SNDBUF override; 0 = kernel default
    std::size_t                                                                                 m_so_rcvbuf{0};   // SO_RCVBUF override; 0 = kernel default
    std::size_t                                                                                 m_dropped{0};     // congestion=drop shed count
    std::size_t                                                                                 m_send_errors{0}; // transient per-datagram send-failure discards
    std::size_t                                                                                 m_recv_resets{0}; // spurious recv connection_reset signals swallowed
    datagram::detail::send_queue<endpoint_type>                                                 m_send_queue;     // byte-capped owned outbound discipline
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
    plexus::detail::move_only_function<void(io::io_error)>                                      m_on_error;
    bool                                                                                        m_open{false};
};

}

#endif
