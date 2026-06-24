#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_TRANSPORT_H

#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"
#include "plexus/asio/stream_transport.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/security/peer_cred_policy.h"

#include <asio/io_context.hpp>

#include <array>
#include <cstddef>
#include <system_error>
#include <string_view>

namespace plexus::asio {

struct unix_transport_traits
{
    static ::asio::local::stream_protocol::endpoint parse(const std::string &address, std::error_code &ec)
    {
        return detail::parse_unix(address, ec);
    }

    static void after_connect(unix_channel &, bool) noexcept
    {
    }
};

class unix_transport : public stream_transport<unix_channel, unix_listener, unix_transport_traits>
{
    using base = stream_transport<unix_channel, unix_listener, unix_transport_traits>;

public:
    explicit unix_transport(::asio::io_context &io, stream::stream_inbound_config cfg = {}, io::congestion congestion = io::congestion::block,
                            io::egress_capacity egress = io::egress_capacity::bounded_default(), stream_socket_options socket_options = {},
                            std::size_t global_default = io::global_default_max_message_bytes, std::size_t reassembly_budget = io::reassembly_memory_budget)
            : base(io, stream::with_message_limits(cfg, global_default, reassembly_budget), false, congestion, egress, socket_options, io,
                   stream::with_message_limits(cfg, global_default, reassembly_budget), unix_listener::default_socket_mode, io::security::shared_accept_any_peer_cred(), congestion,
                   egress, socket_options)
    {
    }

    static constexpr std::array<std::string_view, 1> mux_schemes{"unix"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::local;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::unix_transport, plexus::asio::unix_policy>,
              "unix_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
