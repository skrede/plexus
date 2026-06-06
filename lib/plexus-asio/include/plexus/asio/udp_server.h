#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_SERVER_H

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <deque>
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
// burst / retransmit-vs-send / multi-peer overlap. Instead the caller's bytes are
// copied SYNCHRONOUSLY into an OWNED outbound-queue node (the same write-queue
// discipline the stream channels use), and the queue drains SERIALLY with at most
// one outstanding async_send_to: the completion pops the front and chains the next.
// The owned node captures the FULLY-WRAPPED outer datagram, so every caller-side
// scratch (outer and inner) is only ever read synchronously into the node.
//
// Single-owner, bare `this`, no shared_from_this / strand; the owning udp_transport
// closes the socket before the server dies, so a completion firing after teardown
// self-guards on operation_aborted.
class udp_server
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    explicit udp_server(::asio::io_context &io)
        : m_socket(io)
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

    // Copy the caller's bytes into an owned queue node (synchronously, so the caller's
    // scratch buffer is never referenced live across the async op) and drive the serial
    // drain. The queue node owns the datagram until its completion pops it.
    void send_to(std::span<const std::byte> bytes, const endpoint_type &dest)
    {
        if(!m_open)
            return;
        m_send_queue.push_back(outbound{std::vector<std::byte>(bytes.begin(), bytes.end()), dest});
        if(!m_sending)
            do_send();
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

    void close()
    {
        if(!m_socket.is_open())
            return;
        std::error_code ec;
        (void)m_socket.close(ec);
        m_open = false;
        m_sending = false;
        m_send_queue.clear();
    }

private:
    struct outbound
    {
        std::vector<std::byte> bytes;
        endpoint_type dest;
    };

    // Drain the owned outbound queue serially: send the front node, and on completion
    // pop it and chain the next. At most one async_send_to is outstanding, so a node's
    // bytes stay valid until its own completion runs (the stream-channel write-queue
    // discipline applied to the connectionless socket).
    void do_send()
    {
        if(m_send_queue.empty())
        {
            m_sending = false;
            return;
        }
        m_sending = true;
        const auto &front = m_send_queue.front();
        m_socket.async_send_to(::asio::buffer(front.bytes), front.dest,
            [this](std::error_code ec, std::size_t)
            {
                if(ec)
                    report(ec);
                if(!m_open)
                    return;
                m_send_queue.pop_front();
                do_send();
            });
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
    std::deque<outbound> m_send_queue;          // owned outbound datagrams, drained serially
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_open{false};
    bool m_sending{false};                      // one async_send_to outstanding at a time
};

}

#endif
