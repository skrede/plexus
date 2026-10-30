#ifndef HPP_GUARD_PLEXUS_FREERTOS_UART_CHANNEL_H
#define HPP_GUARD_PLEXUS_FREERTOS_UART_CHANNEL_H

#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/uart_io.h"
#include "plexus/freertos/detail/close_state.h"

#include "plexus/io/byte_channel.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/stream/serial_frame.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <cstddef>
#include <utility>

namespace plexus::freertos {

class uart_channel
{
public:
    static constexpr std::size_t k_scratch_bytes = 2048;

    explicit uart_channel(uart_port_t port, freertos_executor &ex, std::size_t max_payload, std::size_t rx_ring_ceiling)
            : m_port(port)
            , m_close(ex)
            , m_dropped_count(0)
            , m_overrun_count(0)
            , m_rx_ring_ceiling(rx_ring_ceiling)
            , m_decorator(max_payload)
            , m_scratch()
    {
        wire_decorator();
    }

    void send(std::span<const std::byte> framed)
    {
        if(m_close.closed())
            return;
        const auto header  = framed.first(plexus::wire::header_size);
        const auto payload = framed.subspan(plexus::wire::header_size);
        const auto trailer = plexus::stream::crc_trailer(header, payload);
        detail::uart_write_all(m_port, framed);
        detail::uart_write_all(m_port, std::span<const std::byte>{trailer});
    }

    void close()
    {
        if(!m_close.enter())
            return;
        m_on_data_cb           = nullptr;
        m_on_error_cb          = nullptr;
        m_on_protocol_close_cb = nullptr;
        m_close.notify();
    }

    plexus::io::endpoint remote_endpoint() const
    {
        return {"serial", ""};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }

    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_close.install(std::move(cb));
    }

    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

    void poll()
    {
        if(m_close.closed())
            return;
        check_overrun();
        const std::size_t n = detail::uart_poll_into(m_port, m_scratch);
        if(n > 0)
            m_decorator.feed(std::span<const std::byte>{m_scratch.data(), n});
    }

    std::size_t overrun_count() const noexcept
    {
        return m_overrun_count;
    }

    std::size_t dropped_count() const noexcept
    {
        return m_dropped_count;
    }

private:
    uart_port_t m_port;
    detail::close_state m_close;
    std::size_t m_dropped_count;
    std::size_t m_overrun_count;
    std::size_t m_rx_ring_ceiling;
    plexus::stream::crc_serial_inbound m_decorator;
    std::array<std::byte, k_scratch_bytes> m_scratch;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)> m_on_protocol_close_cb;

    void wire_decorator()
    {
        m_decorator.on_match([this](std::span<const std::byte> f) { deliver(f); });
        m_decorator.on_drop([this](plexus::wire::close_cause) { ++m_dropped_count; });
    }

    // Delivery is synchronous because poll runs on the executor's task, so the frame is consumed before poll reuses
    // the scratch; a posted copy would allocate per frame.
    void deliver(std::span<const std::byte> frame)
    {
        if(m_on_data_cb)
            m_on_data_cb(frame);
    }

    void check_overrun()
    {
        if(!detail::uart_ring_overrun(m_port, m_rx_ring_ceiling))
            return;
        ++m_overrun_count;
        if(m_on_error_cb)
            m_on_error_cb(plexus::io::io_error::would_block);
    }
};

static_assert(plexus::io::byte_channel<plexus::freertos::uart_channel>, "uart_channel must satisfy byte_channel — check the seven verbs");

}

#endif
