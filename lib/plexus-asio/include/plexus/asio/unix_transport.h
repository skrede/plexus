#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_TRANSPORT_H

#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"
#include "plexus/asio/stream_transport.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/security/peer_cred_policy.h"

#include <asio/io_context.hpp>

#include <array>
#include <cstddef>
#include <system_error>
#include <string_view>

namespace plexus::asio {

// The AF_UNIX-specific dial deltas the shared stream_transport routes through: the path parse
// and a no-op post-connect (AF_UNIX has no Nagle, so no no_delay step). AF_UNIX is also path-
// addressed, so the transport exposes no port().
struct unix_transport_traits
{
    static auto parse(const std::string &address, std::error_code &ec) { return detail::parse_unix(address, ec); }

    static void after_connect(unix_channel &, bool) noexcept {}
};

// The AF_UNIX connector: stream_transport over unix_channel + unix_listener. cfg is the node-
// level byte-stream hardening config (required-WITH-default) the transport mints every channel
// with.
class unix_transport
    : public stream_transport<unix_channel, unix_listener, unix_transport_traits>
{
    using base = stream_transport<unix_channel, unix_listener, unix_transport_traits>;

public:
    explicit unix_transport(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                            io::congestion congestion = io::congestion::block,
                            std::size_t write_queue_bytes = unix_channel::default_write_queue_bytes,
                            stream_socket_options socket_options = {})
        : base(io, cfg, false, congestion, write_queue_bytes, socket_options,
               io, cfg, unix_listener::default_socket_mode,
               io::security::shared_accept_any_peer_cred(),
               congestion, write_queue_bytes, socket_options)
    {
    }

    // The schemes this member serves + its locality tier: AF_UNIX is the same-host (local)
    // member, serving the "unix" scheme.
    static constexpr std::array<std::string_view, 1> mux_schemes{"unix"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::local;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::unix_transport, plexus::asio::unix_policy>,
    "unix_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
