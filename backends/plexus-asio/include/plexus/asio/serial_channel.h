#ifndef HPP_GUARD_PLEXUS_ASIO_SERIAL_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_SERIAL_CHANNEL_H

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/detail/serial_bootstrap.h"
#include "plexus/asio/detail/serial_channel_io.h"

#include "plexus/stream/crc_serial.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/egress_capacity.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/serial_port.hpp>

#include <span>
#include <vector>
#include <memory>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// The host serial byte_channel: stream_channel over ::asio::serial_port plus the serial-only
// CRC32C integrity decorator. The egress appends a 4-byte CRC32C trailer (serial_bootstrap);
// inbound runs its OWN read loop (serial_do_read) driving a CRC verify+resync decorator instead
// of the shared stream_inbound feed, so a corrupt frame is dropped + magic-resynced (never
// closing the link). A failed CRC surfaces through the non-fatal on_frame_dropped seam, leaving
// on_protocol_close strictly fatal.
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
    explicit serial_channel(::asio::io_context &io, stream::stream_inbound_config cfg = {}, io::congestion congestion = io::congestion::block,
                            io::egress_capacity egress = io::egress_capacity::bounded_default(), stream_socket_options opts = {},
                            std::size_t read_buffer_bytes = k_stream_read_buffer_bytes)
            : base(io, cfg, congestion, egress, opts, read_buffer_bytes)
            , m_decorator(cfg.max_payload_size)
            , m_read_buf(stream_read_buffer_size(read_buffer_bytes))
    {
        wire_decorator();
    }

    serial_channel(::asio::io_context &io, ::asio::serial_port connected, stream::stream_inbound_config cfg = {}, io::congestion congestion = io::congestion::block,
                   io::egress_capacity egress = io::egress_capacity::bounded_default(), stream_socket_options opts = {}, std::size_t read_buffer_bytes = k_stream_read_buffer_bytes)
            : base(io, std::move(connected), cfg, congestion, egress, opts, read_buffer_bytes)
            , m_decorator(cfg.max_payload_size)
            , m_read_buf(stream_read_buffer_size(read_buffer_bytes))
    {
        wire_decorator();
        // The adopt ctor's read loop, deferred until the decorator is constructed.
        start_read();
    }

    // The consumer's on_data is fed the CRC-verified bytes the decorator emits (the base
    // stream_inbound is bypassed on serial).
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }

    // The non-fatal drop seam, distinct from on_protocol_close: a CRC mismatch fires here (the
    // frame was dropped and the link resynced), never tearing the channel down.
    void on_frame_dropped(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_frame_dropped_cb = std::move(cb);
    }

    // Idempotent: the adopt ctor arms the loop once; a later transport start_read() is a no-op.
    void start_read()
    {
        if(m_reading)
            return;
        m_reading = true;
        mark_open();
        detail::serial_do_read(*this);
    }

    // The serial read-loop seam (reached by detail::serial_do_read).
    ::asio::serial_port &serial_stream() noexcept
    {
        return stream();
    }
    std::vector<std::byte> &serial_read_buf() noexcept
    {
        return m_read_buf;
    }
    stream::crc_serial_inbound &serial_decorator() noexcept
    {
        return m_decorator;
    }

private:
    void wire_decorator()
    {
        m_decorator.on_match([this](std::span<const std::byte> f) { post_frame(f); });
        m_decorator.on_drop(
                [this](wire::close_cause c)
                {
                    if(m_on_frame_dropped_cb)
                        m_on_frame_dropped_cb(c);
                });
    }

    // Post an owning copy so the bytes survive the deferred delivery — the read buffer is reused
    // on the next read.
    void post_frame(std::span<const std::byte> frame)
    {
        auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
        ::asio::post(stream().get_executor(),
                     [this, owned = std::move(owned)]
                     {
                         if(m_on_data_cb)
                             m_on_data_cb(std::span<const std::byte>{*owned});
                     });
    }

    stream::crc_serial_inbound m_decorator;
    std::vector<std::byte> m_read_buf;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_frame_dropped_cb;
};

}

static_assert(plexus::io::byte_channel<plexus::asio::serial_channel>, "serial_channel must satisfy byte_channel — check the seven verbs");

#endif
