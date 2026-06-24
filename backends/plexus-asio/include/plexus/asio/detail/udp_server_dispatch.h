#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_SERVER_DISPATCH_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_SERVER_DISPATCH_H

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/datagram/detail/send_queue.h"

#include <asio/ip/udp.hpp>
#include <asio/socket_base.hpp>

#include <span>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::asio::detail {

// A transient send error (an unreachable destination, a momentary buffer-full) is best-effort UDP
// loss the drain ignores; everything in the fatal set means the socket is unusable going forward.
inline bool transient_send_error(const std::error_code &ec) noexcept
{
    return ec != ::asio::error::bad_descriptor && ec != ::asio::error::operation_not_supported && ec != ::asio::error::network_down;
}

template<typename S>
void server_report(S &s, const std::error_code &ec)
{
    if(s.m_on_error)
        s.m_on_error(detail::map_error(ec));
}

// At the byte cap: drop_newest sheds the overrun datagram (counted), block surfaces would_block.
template<typename S>
void on_send_queue_full(S &s)
{
    if(s.m_congestion == io::congestion::drop_newest)
    {
        ++s.m_dropped;
        return;
    }
    if(s.m_on_error)
        s.m_on_error(io::io_error::would_block);
}

template<typename S>
void on_sync_send_error(S &s, const std::error_code &ec)
{
    if(transient_send_error(ec))
    {
        ++s.m_send_errors;
        return;
    }
    server_report(s, ec);
    s.m_send_queue.close();
}

template<typename S>
typename datagram::detail::send_queue<typename S::endpoint_type>::send_sink make_send_sink(S &s)
{
    using endpoint_type = typename S::endpoint_type;
    return [&s](std::span<const std::byte> bytes, const endpoint_type &dest, typename datagram::detail::send_queue<endpoint_type>::completion done)
    {
        s.m_socket.async_send_to(::asio::buffer(bytes.data(), bytes.size()), dest,
                                 [&s, done = std::move(done)](std::error_code ec, std::size_t) mutable
                                 {
                                     if(ec == ::asio::error::operation_aborted || !s.m_open)
                                         return;
                                     if(!ec)
                                         return done(true);
                                     if(transient_send_error(ec))
                                     {
                                         ++s.m_send_errors; // best-effort UDP loss: chain past it
                                         return done(true);
                                     }
                                     server_report(s, ec); // a fatal socket error: report once and stop the chain
                                     s.m_send_queue.close();
                                 });
    };
}

// Between open() and bind(): a 0 override leaves the kernel default; a setsockopt rejection is
// non-fatal (the sizes are a throughput hint, not a correctness requirement).
template<typename S>
void apply_socket_buffers(S &s)
{
    std::error_code ec;
    if(s.m_so_sndbuf != 0)
        (void)s.m_socket.set_option(::asio::socket_base::send_buffer_size(static_cast<int>(s.m_so_sndbuf)), ec);
    if(s.m_so_rcvbuf != 0)
        (void)s.m_socket.set_option(::asio::socket_base::receive_buffer_size(static_cast<int>(s.m_so_rcvbuf)), ec);
}

template<typename S>
void do_receive(S &s);

// A spurious connection_reset (a Windows ICMP port-unreachable surfaced on the next recv) re-arms
// silently; any other recv error is surfaced and the loop re-arms.
template<typename S>
void fail(S &s, const std::error_code &ec)
{
    if(ec == ::asio::error::operation_aborted || !s.m_open)
        return;
    if(ec == ::asio::error::connection_reset)
    {
        ++s.m_recv_resets;
        if(s.m_open)
            do_receive(s);
        return;
    }
    server_report(s, ec);
    if(s.m_open)
        do_receive(s);
}

template<typename S>
void do_receive(S &s)
{
    s.m_socket.async_receive_from(::asio::buffer(s.m_recv_buf), s.m_sender,
                                  [&s](std::error_code ec, std::size_t n)
                                  {
                                      if(ec)
                                          return fail(s, ec);
                                      if(s.m_on_datagram)
                                          s.m_on_datagram(s.m_sender, std::span<const std::byte>{s.m_recv_buf.data(), n});
                                      if(s.m_open)
                                          do_receive(s);
                                  });
}

}

#endif
