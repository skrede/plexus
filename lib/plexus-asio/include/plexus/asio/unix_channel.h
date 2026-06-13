#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_CHANNEL_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/plaintext_bootstrap.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// The AF_UNIX byte_channel: stream_channel over ::asio::local::stream_protocol::socket with
// the plaintext open path. The traits carry the only AF_UNIX-specific lines: the "unix"
// scheme stamp, the path-based endpoint format, and the shutdown_both enum. See
// stream_channel for the shared core.
struct unix_traits
{
    static io::endpoint format_endpoint(const ::asio::local::stream_protocol::socket &sock)
    {
        std::error_code ec;
        auto ep = sock.remote_endpoint(ec);
        if(ec)
            return {"unix", ""};
        return {"unix", ep.path()};
    }

    static void shutdown(::asio::local::stream_protocol::socket &sock, std::error_code &ec)
    {
        (void)sock.shutdown(::asio::local::stream_protocol::socket::shutdown_both, ec);
    }

    static ::asio::local::stream_protocol::socket &lowest_layer(::asio::local::stream_protocol::socket &s) noexcept { return s; }
    static const ::asio::local::stream_protocol::socket &lowest_layer(const ::asio::local::stream_protocol::socket &s) noexcept { return s; }
};

class unix_channel
    : public stream_channel<::asio::local::stream_protocol::socket, unix_traits,
                            detail::plaintext_bootstrap<::asio::local::stream_protocol::socket>>
{
    using base = stream_channel<::asio::local::stream_protocol::socket, unix_traits,
                                detail::plaintext_bootstrap<::asio::local::stream_protocol::socket>>;

public:
    explicit unix_channel(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                          io::congestion congestion = io::congestion::block,
                          std::size_t write_queue_bytes = base::default_write_queue_bytes)
        : base(io, cfg, congestion, write_queue_bytes)
    {
    }

    unix_channel(::asio::io_context &io, ::asio::local::stream_protocol::socket connected,
                 wire::stream_inbound_config cfg = {},
                 io::congestion congestion = io::congestion::block,
                 std::size_t write_queue_bytes = base::default_write_queue_bytes)
        : base(io, std::move(connected), cfg, congestion, write_queue_bytes)
    {
    }
};

}

static_assert(plexus::io::byte_channel<plexus::asio::unix_channel>,
    "unix_channel must satisfy byte_channel — check the seven verbs");

#endif
