#ifndef HPP_GUARD_PLEXUS_EXAMPLE_BENCH_UART_H
#define HPP_GUARD_PLEXUS_EXAMPLE_BENCH_UART_H

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/mcu_byte_owner.h"
#include "plexus/freertos/uart_channel.h"

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

// The serial-cell Policy triple: identical to the freertos policy except byte_channel_type binds
// to the real uart_channel. Fully-qualify plexus::detail:: below — an io/host-shim detail namespace
// is in scope and would shadow the bare detail:: lookup (the move_only_function shadowing pitfall).
struct bench_uart_policy
{
    using executor_type     = plexus::freertos::freertos_executor &;
    using byte_channel_type = plexus::freertos::uart_channel;
    using timer_type        = plexus::freertos::freertos_timer;
    using byte_owner        = plexus::freertos::mcu_byte_owner;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<bench_uart_policy>, "bench_uart_policy must satisfy Policy");

inline constexpr std::size_t k_uart_max_payload = 8 * 1024;
inline constexpr std::size_t k_uart_rx_ring     = 8 * 1024;

// The plexus serial link rides UART1 so the console stays on UART0: the bench emits its
// machine-parseable sample lines on the console while the framed plexus bytes flow over a separate
// pair of pads to a second USB-serial adapter on the host. UART0 is never shared with the link.
inline constexpr uart_port_t k_link_uart   = UART_NUM_1;
inline constexpr int         k_link_tx_gpio = 17;
inline constexpr int         k_link_rx_gpio = 16;

// The on-device serial connector for the bench cell: a point-to-point UART implements
// transport_backend directly, both verbs collapsing to install-driver-and-deliver-ONE-channel. By
// convention the dialing peer (the host) drives the handshake; this device listens.
class bench_uart_transport
{
public:
    bench_uart_transport() = default;

    bench_uart_transport(const bench_uart_transport &)            = delete;
    bench_uart_transport &operator=(const bench_uart_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<plexus::freertos::uart_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<plexus::freertos::uart_channel>, const plexus::io::endpoint &)> cb)
    {
        m_on_dialed = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void listen(const plexus::io::endpoint &)
    {
        open_driver();
        if(m_on_accepted)
            m_on_accepted(mint_channel());
    }

    void dial(const plexus::io::endpoint &ep)
    {
        open_driver();
        if(m_on_dialed)
            m_on_dialed(mint_channel(), ep);
    }

    void poll()
    {
        if(m_channel)
            m_channel->poll();
    }

    void close()
    {
        if(m_installed)
        {
            uart_driver_delete(k_link_uart);
            m_installed = false;
        }
    }

    static constexpr std::array<std::string_view, 1> mux_schemes{"serial"};
    static constexpr plexus::io::transport_kind mux_tier = plexus::io::transport_kind::local;

private:
    std::unique_ptr<plexus::freertos::uart_channel> mint_channel()
    {
        auto ch   = std::make_unique<plexus::freertos::uart_channel>(k_link_uart, k_uart_max_payload, k_uart_rx_ring);
        m_channel = ch.get();
        return ch;
    }

    void open_driver()
    {
        if(m_installed)
            return;
        const uart_config_t cfg{
            .baud_rate           = 115200,
            .data_bits           = UART_DATA_8_BITS,
            .parity              = UART_PARITY_DISABLE,
            .stop_bits           = UART_STOP_BITS_1,
            .flow_ctrl           = UART_HW_FLOWCTRL_DISABLE,
            .rx_flow_ctrl_thresh = 0,
            .source_clk          = UART_SCLK_DEFAULT,
            .flags               = {},
        };
        uart_param_config(k_link_uart, &cfg);
        uart_set_pin(k_link_uart, k_link_tx_gpio, k_link_rx_gpio, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE);
        uart_driver_install(k_link_uart, k_uart_rx_ring, 0, 0, nullptr, 0);
        m_installed = true;
    }

    bool                            m_installed{false};
    plexus::freertos::uart_channel *m_channel{nullptr};
    plexus::detail::move_only_function<void(std::unique_ptr<plexus::freertos::uart_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<plexus::freertos::uart_channel>, const plexus::io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error;
};

static_assert(plexus::io::transport_backend<example::bench_uart_transport, example::bench_uart_policy>,
              "bench_uart_transport must satisfy transport_backend — check listen/dial/on_* surface");

}

#endif
