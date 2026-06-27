#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_MULTICAST_SOCKET_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_MULTICAST_SOCKET_H

#include "plexus/asio/detail/multicast_join.h"
#include "plexus/asio/detail/udp_server_dispatch.h"

#include "plexus/stream/datagram_socket.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/datagram/detail/send_queue.h"
#include "plexus/detail/compat.h"

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::asio {

// A structural sibling of udp_server (NOT a subclass): the same member names let the S-generic
// plexus::asio::detail dispatch templates (do_receive/make_send_sink/server_report/...) bind to it
// verbatim, so udp_server stays unmuddied. It adds the one new mechanism — the IPv4 IGMP join — and
// sends to the fixed group/port. Connectionless: one socket, every sender on the group delivered
// through on_datagram with its kernel source endpoint (the unspoofable sender discovery maps).
class udp_multicast_socket
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    explicit udp_multicast_socket(::asio::io_context &io, ::asio::ip::address_v4 group, std::uint16_t port, unsigned ttl, io::congestion congestion = io::congestion::block,
                                  std::size_t send_queue_bytes = 65536)
            : m_socket(io)
            , m_sender{}
            , m_recv_buf{}
            , m_congestion(congestion)
            , m_so_sndbuf(0)
            , m_so_rcvbuf(0)
            , m_dropped(0)
            , m_send_errors(0)
            , m_recv_resets(0)
            , m_send_queue(detail::make_send_sink(*this), send_queue_bytes)
            , m_group(group)
            , m_port(port)
            , m_ttl(ttl)
            , m_open(false)
    {
    }

    udp_multicast_socket(const udp_multicast_socket &)            = delete;
    udp_multicast_socket &operator=(const udp_multicast_socket &) = delete;

    ~udp_multicast_socket()
    {
        close();
    }

    // Fail-closed: an open/join error is reported through on_error and the recv loop is never armed.
    // The bind_ep protocol selects the family; the join helper does the address-level bind to ANY.
    std::error_code bind(const endpoint_type &bind_ep)
    {
        std::error_code ec;
        m_socket.open(bind_ep.protocol(), ec);
        if(ec)
            return detail::server_report(*this, ec), ec;
        (void)m_socket.non_blocking(true, ec);
        detail::join_multicast_group(m_socket, m_group, m_port, m_ttl, ec);
        if(ec)
            return detail::server_report(*this, ec), ec;
        m_open = true;
        detail::do_receive(*this);
        return ec;
    }

    void send_multicast(std::span<const std::byte> bytes)
    {
        if(!m_open)
            return;
        if(!m_send_queue.enqueue(bytes, endpoint_type{m_group, m_port}))
            detail::on_send_queue_full(*this);
    }

    void on_datagram(plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram_cb = std::move(cb);
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
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

    ::asio::ip::udp::socket m_socket;
    endpoint_type m_sender;
    std::array<std::byte, 65536> m_recv_buf;
    io::congestion m_congestion;
    std::size_t m_so_sndbuf;
    std::size_t m_so_rcvbuf;
    std::size_t m_dropped;
    std::size_t m_send_errors;
    std::size_t m_recv_resets;
    datagram::detail::send_queue<endpoint_type> m_send_queue;
    ::asio::ip::address_v4 m_group;
    std::uint16_t m_port;
    unsigned m_ttl;
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram_cb;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    bool m_open;
};

static_assert(stream::datagram_socket<udp_multicast_socket>);

}

#endif
