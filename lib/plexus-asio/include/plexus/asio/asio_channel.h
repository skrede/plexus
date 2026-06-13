#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/plaintext_bootstrap.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"

#include <asio/socket_base.hpp>
#include <asio/basic_socket.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <string>
#include <utility>
#include <cstddef>
#include <system_error>

#if defined(__linux__) || defined(__APPLE__)
#include <netinet/in.h>
#include <netinet/tcp.h>
#elif defined(_WIN32)
#include <winsock2.h>
#include <mstcpip.h>
#endif

namespace plexus::asio {

// The TCP byte_channel: stream_channel over ::asio::ip::tcp::socket with the plaintext
// open path (sends reach the egress directly — no handshake gate). The traits carry the
// only TCP-specific lines: the "tcp" scheme stamp, the host:port endpoint format, and the
// shutdown_both enum. See stream_channel for the shared core + the on_data/on_protocol_close
// contract.
struct tcp_traits
{
    static io::endpoint format_endpoint(const ::asio::ip::tcp::socket &sock)
    {
        std::error_code ec;
        auto ep = sock.remote_endpoint(ec);
        if(ec)
            return {"tcp", ""};
        return {"tcp", ep.address().to_string() + ":" + std::to_string(ep.port())};
    }

    static void shutdown(::asio::ip::tcp::socket &sock, std::error_code &ec)
    {
        (void)sock.shutdown(::asio::ip::tcp::socket::shutdown_both, ec);
    }

    static ::asio::ip::tcp::socket &lowest_layer(::asio::ip::tcp::socket &s) noexcept { return s; }
    static const ::asio::ip::tcp::socket &lowest_layer(const ::asio::ip::tcp::socket &s) noexcept { return s; }

    // Apply the portable buffer + keepalive knobs through asio (carries to every platform),
    // then the granular keepalive intervals ONLY when the operator opted in — those have no
    // portable asio option, so they go through the native handle behind a per-platform guard
    // (only the Linux path is exercised on this host; macOS/Windows are compile-guarded and
    // proven on-platform later). Every set is best-effort: a clamped/rejected knob keeps the
    // socket usable, so the ec is swallowed (a buffer size is a throughput hint). The socket
    // is taken as the lowest-layer basic_socket so the TLS channel routes its TCP lowest layer
    // through this same hook (its lowest_layer_type IS basic_socket<tcp>, not tcp::socket).
    static void apply_socket_options(::asio::basic_socket<::asio::ip::tcp> &sock,
                                     const stream_socket_options &opts, std::error_code &ec)
    {
        if(opts.so_sndbuf != 0)
            (void)sock.set_option(::asio::socket_base::send_buffer_size(static_cast<int>(opts.so_sndbuf)), ec);
        if(opts.so_rcvbuf != 0)
            (void)sock.set_option(::asio::socket_base::receive_buffer_size(static_cast<int>(opts.so_rcvbuf)), ec);
        if(!opts.keepalive)
            return;
        (void)sock.set_option(::asio::socket_base::keep_alive(true), ec);
        apply_keepalive_intervals(sock.native_handle(), opts);
    }

private:
    static void apply_keepalive_intervals(int fd, const stream_socket_options &opts) noexcept
    {
#if defined(__linux__)
        set_keepalive_secs(fd, IPPROTO_TCP, TCP_KEEPIDLE, opts.keepalive_idle_secs);
        set_keepalive_secs(fd, IPPROTO_TCP, TCP_KEEPINTVL, opts.keepalive_interval_secs);
        set_keepalive_secs(fd, IPPROTO_TCP, TCP_KEEPCNT, opts.keepalive_count);
#elif defined(__APPLE__)
        set_keepalive_secs(fd, IPPROTO_TCP, TCP_KEEPALIVE, opts.keepalive_idle_secs);
        set_keepalive_secs(fd, IPPROTO_TCP, TCP_KEEPINTVL, opts.keepalive_interval_secs);
        set_keepalive_secs(fd, IPPROTO_TCP, TCP_KEEPCNT, opts.keepalive_count);
#elif defined(_WIN32)
        if(opts.keepalive_idle_secs == 0 && opts.keepalive_interval_secs == 0)
            return;
        ::tcp_keepalive vals{};
        vals.onoff = 1;
        vals.keepalivetime = opts.keepalive_idle_secs * 1000u;
        vals.keepaliveinterval = opts.keepalive_interval_secs * 1000u;   // SIO_KEEPALIVE_VALS carries no count
        DWORD written = 0;
        (void)::WSAIoctl(static_cast<SOCKET>(fd), SIO_KEEPALIVE_VALS, &vals, sizeof(vals),
                         nullptr, 0, &written, nullptr, nullptr);
#else
        (void)fd; (void)opts;
#endif
    }

#if defined(__linux__) || defined(__APPLE__)
    static void set_keepalive_secs(int fd, int level, int name, std::uint32_t secs) noexcept
    {
        if(secs == 0)
            return;
        const int value = static_cast<int>(secs);
        (void)::setsockopt(fd, level, name, &value, sizeof(value));
    }
#endif
};

class asio_channel
    : public stream_channel<::asio::ip::tcp::socket, tcp_traits,
                            detail::plaintext_bootstrap<::asio::ip::tcp::socket>>
{
    using base = stream_channel<::asio::ip::tcp::socket, tcp_traits,
                                detail::plaintext_bootstrap<::asio::ip::tcp::socket>>;

public:
    explicit asio_channel(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                          io::congestion congestion = io::congestion::block,
                          std::size_t write_queue_bytes = base::default_write_queue_bytes,
                          stream_socket_options opts = {})
        : base(io, cfg, congestion, write_queue_bytes, opts)
    {
    }

    asio_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected,
                 wire::stream_inbound_config cfg = {},
                 io::congestion congestion = io::congestion::block,
                 std::size_t write_queue_bytes = base::default_write_queue_bytes,
                 stream_socket_options opts = {})
        : base(io, std::move(connected), cfg, congestion, write_queue_bytes, opts)
    {
    }
};

}

static_assert(plexus::io::byte_channel<plexus::asio::asio_channel>,
    "asio_channel must satisfy byte_channel — check the seven verbs");

#endif
