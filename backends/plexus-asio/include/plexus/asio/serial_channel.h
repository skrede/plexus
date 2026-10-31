#ifndef HPP_GUARD_PLEXUS_ASIO_SERIAL_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_SERIAL_CHANNEL_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/serial_bootstrap.h"
#include "plexus/asio/detail/serial_channel_io.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/stream/serial_frame.h"
#include "plexus/stream/stream_inbound.h"

#include "plexus/wire_bytes.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/detail/drop_event.h"

#include <asio/io_context.hpp>
#include <asio/serial_port.hpp>

#include <span>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::asio {

// Serial applies no socket options and never feeds the shared stream inbound, so neither has a knob here.
struct serial_channel_options
{
    std::size_t max_payload{stream::k_serial_default_max_payload};
    io::congestion congestion{io::congestion::block};
    io::egress_capacity egress{io::egress_capacity::bounded_default()};
    std::size_t read_buffer_bytes{k_stream_read_buffer_bytes};
};

// The serial channel runs its own read loop through the serial deframer instead of the shared stream feed. A refused
// frame is non-fatal and surfaces on on_frame_dropped, while on_protocol_close stays fatal.
struct serial_traits
{
    static io::endpoint format_endpoint(const ::asio::serial_port &)
    {
        return {"serial", ""};
    }

    static void shutdown(::asio::serial_port &port, std::error_code &ec)
    {
        (void)port.close(ec);
    }

    static ::asio::serial_port &lowest_layer(::asio::serial_port &p) noexcept
    {
        return p;
    }
    static const ::asio::serial_port &lowest_layer(const ::asio::serial_port &p) noexcept
    {
        return p;
    }

    static void apply_socket_options(::asio::serial_port &, const stream_socket_options &, std::error_code &) noexcept
    {
    }
};

class serial_channel : public stream_channel<::asio::serial_port, serial_traits, detail::serial_bootstrap<::asio::serial_port>>
{
    using base = stream_channel<::asio::serial_port, serial_traits, detail::serial_bootstrap<::asio::serial_port>>;

public:
    explicit serial_channel(::asio::io_context &io, serial_channel_options opts = {})
            : base(io, inbound_config(opts.max_payload), opts.congestion, opts.egress, stream_socket_options{}, opts.read_buffer_bytes)
            , m_decorator(opts.max_payload)
    {
        detail::serial_wire_decorator(*this);
    }

    serial_channel(::asio::io_context &io, ::asio::serial_port connected, serial_channel_options opts = {})
            : base(io, std::move(connected), inbound_config(opts.max_payload), opts.congestion, opts.egress, stream_socket_options{}, opts.read_buffer_bytes)
            , m_decorator(opts.max_payload)
    {
        detail::serial_wire_decorator(*this);
        // The adopt ctor's read loop, deferred until the decorator is constructed.
        start_read();
    }

    void send(std::span<const std::byte> data)
    {
        if(data.size() > max_frame_bytes())
            return detail::serial_refuse_oversize(*this);
        base::send(data);
    }

    void send(wire_bytes<> data)
    {
        if(data.size() > max_frame_bytes())
            return detail::serial_refuse_oversize(*this);
        base::send(std::move(data));
    }

    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    // The non-fatal drop seam, distinct from on_protocol_close: every frame the deframer refuses fires here with its
    // cause, and the link resyncs without tearing the channel down.
    void on_frame_dropped(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_frame_dropped_cb = std::move(cb);
    }

    // Idempotent: the adopt ctor arms the loop once; a later transport start_read() is a no-op.
    void start_read()
    {
        if(m_reading || !stream().is_open())
            return;
        m_reading = true;
        mark_open();
        detail::serial_do_read(*this);
    }

    // The largest wire frame this leg carries; the peer's deframer must run the same payload cap, or it refuses what
    // this end sends.
    std::size_t max_frame_bytes() const noexcept
    {
        return stream::serial_frame_ceiling(m_decorator.max_payload());
    }

private:
    template<typename Ch>
    friend void detail::serial_do_read(Ch &);
    template<typename Ch>
    friend void detail::serial_post_frame(Ch &, std::span<const std::byte>);
    template<typename Ch>
    friend void detail::serial_wire_decorator(Ch &);
    template<typename Ch>
    friend void detail::serial_refuse_oversize(Ch &);

    // These die before the base closes the port, which is safe because a channel is destroyed only after its last
    // operation completed.
    stream::crc_serial_inbound m_decorator;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_frame_dropped_cb;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop_cb;

    // The base's stream inbound is never fed on this leg, so its limits only have to agree with the deframer's.
    static stream::stream_inbound_config inbound_config(std::size_t max_payload) noexcept
    {
        return stream::with_message_limits({}, max_payload, io::reassembly_memory_budget);
    }
};

}

static_assert(plexus::io::byte_channel<plexus::asio::serial_channel>, "serial_channel must satisfy byte_channel — check the seven verbs");

#endif
