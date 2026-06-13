#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/plaintext_bootstrap.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <string>
#include <utility>
#include <cstddef>
#include <system_error>

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
                          std::size_t write_queue_bytes = base::default_write_queue_bytes)
        : base(io, cfg, congestion, write_queue_bytes)
    {
    }

    asio_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected,
                 wire::stream_inbound_config cfg = {},
                 io::congestion congestion = io::congestion::block,
                 std::size_t write_queue_bytes = base::default_write_queue_bytes)
        : base(io, std::move(connected), cfg, congestion, write_queue_bytes)
    {
    }
};

}

static_assert(plexus::io::byte_channel<plexus::asio::asio_channel>,
    "asio_channel must satisfy byte_channel — check the seven verbs");

#endif
