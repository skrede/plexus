#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
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
    // path): the shared server queue holds roughly one max-UDP datagram in flight while a
    // saturating publisher's overrun is refused at the bound — the same shallow
    // socket-facing budget the stream channels carry. A load-bearing knob, to be
    // substantiated at the fan-out benchmark.
    static constexpr std::size_t default_send_queue_bytes = 65536;

    // The congestion mode is the per-node QoS choice (block = the safe reliable default
    // that surfaces would_block at the cap; drop_newest = the opt-out shed), threaded with
    // the byte budget as required-WITH-default ctor args exactly as the stream channels.
    explicit udp_server(::asio::io_context &io, io::congestion congestion = io::congestion::block,
                        std::size_t send_queue_bytes = default_send_queue_bytes)
        : m_socket(io)
        , m_congestion(congestion)
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
    // The count of datagrams shed under congestion=drop_newest at the byte cap.
    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_dropped; }

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
    // outstanding (the block's serial discipline), so a node's bytes stay valid until
    // its own completion runs. A send error is surfaced through report; the !m_open
    // guard stops the drain after teardown.
    io::detail::send_queue<endpoint_type>::send_sink make_send_sink()
    {
        return [this](std::span<const std::byte> bytes, const endpoint_type &dest,
                      io::detail::send_queue<endpoint_type>::completion done)
        {
            m_socket.async_send_to(::asio::buffer(bytes.data(), bytes.size()), dest,
                [this, done = std::move(done)](std::error_code ec, std::size_t) mutable
                {
                    if(ec)
                        report(ec);
                    if(!m_open)
                        return;
                    done(!ec);
                });
        };
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
    std::size_t m_dropped{0};                             // congestion=drop shed count
    io::detail::send_queue<endpoint_type> m_send_queue;   // byte-capped owned outbound discipline
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_open{false};
};

}

#endif
