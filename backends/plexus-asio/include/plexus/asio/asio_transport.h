#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_TRANSPORT_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"
#include "plexus/asio/stream_transport.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <system_error>
#include <string_view>

namespace plexus::asio {

struct tcp_transport_traits
{
    static ::asio::ip::tcp::endpoint parse(const std::string &address, std::error_code &ec)
    {
        return detail::parse(address, ec);
    }

    static void after_connect(asio_channel &ch, bool no_delay)
    {
        if(!no_delay)
            return;
        std::error_code nec;
        (void)ch.socket().set_option(::asio::ip::tcp::no_delay(true), nec);
    }
};

class asio_transport : public stream_transport<asio_channel, asio_listener, tcp_transport_traits>
{
    using base = stream_transport<asio_channel, asio_listener, tcp_transport_traits>;

public:
    explicit asio_transport(::asio::io_context &io, stream::stream_inbound_config cfg = {}, bool no_delay = true, io::congestion congestion = io::congestion::block,
                            io::egress_capacity egress = io::egress_capacity::bounded_default(), stream_socket_options socket_options = {},
                            std::size_t global_default = io::global_default_max_message_bytes, std::size_t reassembly_budget = io::reassembly_memory_budget)
            : base(io, stream::with_message_limits(cfg, global_default, reassembly_budget), no_delay, congestion, egress, socket_options, io,
                   stream::with_message_limits(cfg, global_default, reassembly_budget), no_delay, congestion, egress, socket_options)
    {
    }

    static constexpr std::array<std::string_view, 1> mux_schemes{"tcp"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::remote;

    uint16_t port() const
    {
        return listener().port();
    }
};

}

static_assert(plexus::io::transport_backend<plexus::asio::asio_transport, plexus::asio::asio_policy>,
              "asio_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
