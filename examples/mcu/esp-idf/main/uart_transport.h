#ifndef HPP_GUARD_PLEXUS_EXAMPLE_UART_TRANSPORT_H
#define HPP_GUARD_PLEXUS_EXAMPLE_UART_TRANSPORT_H

// Example-side glue for the on-device serial slice. Two pieces live here, both in the
// EXAMPLE component (never in lib/): the Policy binding that swaps the stub channel for
// the real uart_channel, and a thin transport that opens UART0 and delivers ONE channel
// to the node engine. They are example-side ON PURPOSE — the only compiled comms-library
// leaf stays the uart_channel; the binding + transport keep the engine-edit blast radius
// minimal. Both mirror the host serial_transport's no-acceptor point-to-point shape.

#include "plexus/mcu/freertos_timer.h"
#include "plexus/mcu/freertos_executor.h"
#include "plexus/mcu/mcu_byte_owner.h"
#include "plexus/mcu/uart_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include "driver/uart.h"

#include <array>
#include <memory>
#include <utility>
#include <cstddef>
#include <string_view>

namespace example {

// The Policy triple mirroring freertos_policy exactly, EXCEPT byte_channel_type binds to
// the real plexus::mcu::uart_channel (the stub is swapped out for the live serial leaf).
// Fully-qualify plexus::detail:: below: an io/host-shim detail namespace is in scope and
// would shadow the bare detail:: lookup (the move_only_function shadowing pitfall).
struct uart_policy
{
    using executor_type     = plexus::mcu::freertos_executor &;
    using byte_channel_type = plexus::mcu::uart_channel;
    using timer_type        = plexus::mcu::freertos_timer;
    using byte_owner        = plexus::mcu::mcu_byte_owner;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<uart_policy>,
              "uart_policy must satisfy Policy — check the channel/timer/post() seam");

// Per-message ceiling dialed small for the constrained target, and the RX ring sized to
// the driver install below. The ring ceiling is the channel's overrun probe threshold.
inline constexpr std::size_t k_max_payload_bytes = 256;
inline constexpr std::size_t k_rx_ring_bytes     = 2048;

// The on-device serial connector. A UART is point-to-point: there is no acceptor and no
// async connect, so this implements transport_backend DIRECTLY and both verbs collapse to
// install-driver-and-deliver-ONE-channel — listen via on_accepted (no endpoint to
// correlate an inbound channel to), dial via on_dialed carrying the endpoint. By
// convention the dialing end drives the handshake request; this device listens and the
// host gate dials. close() releases the driver; the delivered channel owns its lifetime.
class uart_transport
{
public:
    explicit uart_transport(uart_port_t port = UART_NUM_0)
            : m_port(port)
    {
    }

    uart_transport(const uart_transport &)            = delete;
    uart_transport &operator=(const uart_transport &) = delete;

    void on_accepted(
            plexus::detail::move_only_function<void(std::unique_ptr<plexus::mcu::uart_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(
                           std::unique_ptr<plexus::mcu::uart_channel>, const plexus::io::endpoint &)>
                           cb)
    {
        m_on_dialed = std::move(cb);
    }
    void on_dial_failed(
            plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>
                    cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    // A listen opens the port with no acceptor: the one channel arrives via on_accepted.
    void listen(const plexus::io::endpoint &)
    {
        open_driver();
        if(m_on_accepted)
            m_on_accepted(mint_channel());
    }

    // A dial delivers the same single channel, carrying the endpoint back as the engine's
    // correlation key (the convention the host gate's dial side mirrors).
    void dial(const plexus::io::endpoint &ep)
    {
        open_driver();
        if(m_on_dialed)
            m_on_dialed(mint_channel(), ep);
    }

    // The cooperative RX step the super-loop drives once per iteration. The engine owns the
    // delivered channel (it moved the unique_ptr into a session), but the MCU channel's poll
    // — uart_read_bytes(...,0) -> decorator.feed -> on_data -> engine — is a same-task step
    // the generic engine does not arm (the host channels self-arm async reads). So the
    // transport keeps a NON-OWNING handle to the channel it minted and forwards the poll. The
    // handle stays valid for the whole run: node, transport, and super-loop never return.
    void poll()
    {
        if(m_channel)
            m_channel->poll();
    }

    // Release the driver; the delivered channel has stopped touching the port by teardown.
    void close()
    {
        if(m_installed)
        {
            uart_driver_delete(m_port);
            m_installed = false;
        }
    }

    // The scheme + locality tier: a directly-attached point-to-point UART is the most-local
    // link, serving the "serial" scheme — the same advertisement as the host serial_transport.
    static constexpr std::array<std::string_view, 1> mux_schemes{"serial"};
    static constexpr plexus::io::transport_kind      mux_tier = plexus::io::transport_kind::local;

private:
    // Construct the one channel, record a non-owning handle for poll(), then hand ownership to
    // the caller (the engine moves it into a session). The raw handle never owns.
    std::unique_ptr<plexus::mcu::uart_channel> mint_channel()
    {
        auto ch    = std::make_unique<plexus::mcu::uart_channel>(m_port, k_max_payload_bytes,
                                                                 k_rx_ring_bytes);
        m_channel  = ch.get();
        return ch;
    }

    // Install the ISR-fed RX ring (above the 128-byte hw FIFO) and apply 8N1 @115200 with
    // flow control off — plexus owns UART0 (console NONE), single cable, no spare flow pins.
    // tx_buffer_size==0 makes uart_write_bytes synchronous (deterministic for the slice).
    void open_driver()
    {
        if(m_installed)
            return;
        const uart_config_t cfg{
                .baud_rate  = 115200,
                .data_bits  = UART_DATA_8_BITS,
                .parity     = UART_PARITY_DISABLE,
                .stop_bits  = UART_STOP_BITS_1,
                .flow_ctrl  = UART_HW_FLOWCTRL_DISABLE,
                .source_clk = UART_SCLK_DEFAULT,
        };
        uart_param_config(m_port, &cfg);
        uart_driver_install(m_port, k_rx_ring_bytes, 0, 0, nullptr, 0);
        m_installed = true;
    }

    uart_port_t                  m_port;
    bool                         m_installed{false};
    plexus::mcu::uart_channel   *m_channel{nullptr}; // non-owning poll handle; engine owns it
    plexus::detail::move_only_function<void(std::unique_ptr<plexus::mcu::uart_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<plexus::mcu::uart_channel>,
                                            const plexus::io::endpoint &)>
                                                                                       m_on_dialed;
    plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>
            m_on_dial_failed;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error;
};

static_assert(plexus::io::transport_backend<example::uart_transport, example::uart_policy>,
              "uart_transport must satisfy transport_backend — check listen/dial/on_* surface");

}

#endif
