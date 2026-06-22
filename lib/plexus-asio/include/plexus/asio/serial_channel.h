#ifndef HPP_GUARD_PLEXUS_ASIO_SERIAL_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_SERIAL_CHANNEL_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/plaintext_bootstrap.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/egress_capacity.h"

#include <asio/io_context.hpp>
#include <asio/serial_port.hpp>

#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// The host serial byte_channel: stream_channel over ::asio::serial_port with the plaintext
// open path. Structurally the 4th instantiation of the shared stream_channel core (after TCP,
// AF_UNIX, TLS) — a clone of unix_channel with the socket type swapped. The traits carry the
// only serial-specific lines: the "serial" scheme stamp, the (close-only) shutdown, and the
// no-op socket-options hook (a serial_port's line discipline — baud/flow-control/character-size
// — is applied once at open by serial_transport, BEFORE the port is adopted into the channel,
// so the channel's open-transition hook has nothing left to do). See stream_channel for the
// shared core.
struct serial_traits
{
    // A serial_port has no remote_endpoint (a UART is point-to-point, not address-bearing), so
    // the channel reports the scheme with an empty address — the mux keys on the "serial" scheme,
    // not a peer string. Mirrors unix_traits's error fallback shape.
    static io::endpoint format_endpoint(const ::asio::serial_port &) { return {"serial", ""}; }

    // A serial_port has no shutdown_both half-close — close() is the only teardown verb (the one
    // genuine Traits divergence from unix_traits, which calls socket.shutdown(shutdown_both)).
    static void shutdown(::asio::serial_port &port, std::error_code &ec) { (void)port.close(ec); }

    static ::asio::serial_port &lowest_layer(::asio::serial_port &p) noexcept { return p; }
    static const ::asio::serial_port &lowest_layer(const ::asio::serial_port &p) noexcept
    {
        return p;
    }

    // An explicit no-op: the line discipline (baud_rate / flow_control / character_size) is set
    // once on the open port by serial_transport before the port is adopted, so there is nothing
    // for the channel's open-transition hook to apply. The shared stream_socket_options knobs
    // (SO_SNDBUF/keepalive) do not exist on a serial line.
    static void apply_socket_options(::asio::serial_port &, const stream_socket_options &,
                                     std::error_code &) noexcept
    {
    }
};

class serial_channel
        : public stream_channel<::asio::serial_port, serial_traits,
                                detail::plaintext_bootstrap<::asio::serial_port>>
{
    using base = stream_channel<::asio::serial_port, serial_traits,
                                detail::plaintext_bootstrap<::asio::serial_port>>;

public:
    explicit serial_channel(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                            io::congestion        congestion = io::congestion::block,
                            io::egress_capacity   egress = io::egress_capacity::bounded_default(),
                            stream_socket_options opts   = {},
                            std::size_t           read_buffer_bytes = k_stream_read_buffer_bytes)
            : base(io, cfg, congestion, egress, opts, read_buffer_bytes)
    {
    }

    serial_channel(::asio::io_context &io, ::asio::serial_port connected,
                   wire::stream_inbound_config cfg        = {},
                   io::congestion              congestion = io::congestion::block,
                   io::egress_capacity         egress     = io::egress_capacity::bounded_default(),
                   stream_socket_options       opts       = {},
                   std::size_t                 read_buffer_bytes = k_stream_read_buffer_bytes)
            : base(io, std::move(connected), cfg, congestion, egress, opts, read_buffer_bytes)
    {
    }
};

}

static_assert(plexus::io::byte_channel<plexus::asio::serial_channel>,
              "serial_channel must satisfy byte_channel — check the seven verbs");

#endif
