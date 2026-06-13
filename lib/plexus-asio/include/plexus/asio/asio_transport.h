#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_TRANSPORT_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"
#include "plexus/asio/stream_transport.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <array>
#include <cstdint>
#include <system_error>
#include <string_view>

namespace plexus::asio {

// The TCP-specific dial deltas the shared stream_transport routes through: the host:port
// parse and the post-connect Nagle disable (AF_UNIX has neither).
struct tcp_transport_traits
{
    static auto parse(const std::string &address, std::error_code &ec) { return detail::parse(address, ec); }

    static void after_connect(asio_channel &ch, bool no_delay)
    {
        if(!no_delay)
            return;
        std::error_code nec;
        (void)ch.socket().set_option(::asio::ip::tcp::no_delay(true), nec);   // disable Nagle pre-read
    }
};

// The asio connector: stream_transport over asio_channel + asio_listener. cfg is the node-
// level byte-stream hardening config (required-WITH-default) the transport mints every
// channel with; no_delay disables Nagle on every dialed + accepted TCP socket (required-
// WITH-default true — the latency-MW default; overridable for a Nagle use-case).
class asio_transport
    : public stream_transport<asio_channel, asio_listener, tcp_transport_traits>
{
    using base = stream_transport<asio_channel, asio_listener, tcp_transport_traits>;

public:
    explicit asio_transport(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                            bool no_delay = true)
        : base(io, cfg, no_delay, io, cfg, no_delay)
    {
    }

    // The schemes this member serves + its locality tier: a generic multiplexer reads these
    // at compile time to route by scheme over a member pack — plain TCP is a remote stream
    // member, serving the "tcp" scheme.
    static constexpr std::array<std::string_view, 1> mux_schemes{"tcp"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::remote;

    [[nodiscard]] uint16_t port() const { return listener().port(); }
};

}

static_assert(plexus::io::transport_backend<plexus::asio::asio_transport, plexus::asio::asio_policy>,
    "asio_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
