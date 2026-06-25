// The UART-only node compile gate: a single non-shm transport leaf composed into the full node
// facade, built with plexus-shm OFF the include path (this target links plexus::plexus +
// plexus::core alone). The proof is STRUCTURAL — it must BUILD and LINK naming no shm:: symbol; a
// facade reaching an shm header on this plain pack would fail header-not-found. The publisher
// declared below exercises the now-opaque per-topic geometry override path (a const void* at the
// declare seam), proving it compiles with no shm geometry type in scope.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/transport_backend.h"

#include "plexus/policy.h"
#include "plexus/node_id.h"

#include "plexus/wire/close_cause.h"

#include <span>
#include <deque>
#include <memory>
#include <chrono>
#include <cstddef>
#include <utility>
#include <system_error>

namespace uart_gate {

namespace pio = plexus::io;

// A no-op byte_channel over the serial link: every member the byte_channel concept names, inert.
class uart_channel
{
public:
    void send(std::span<const std::byte>)
    {
    }
    void close()
    {
    }
    pio::endpoint remote_endpoint() const
    {
        return {};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(pio::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

private:
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void()> m_on_closed_cb;
    plexus::detail::move_only_function<void(pio::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)> m_on_protocol_close_cb;
};

// A cooperative step-executor the node borrows by reference: post() queues, pump() drains one.
class uart_executor
{
public:
    void post(plexus::detail::move_only_function<void()> fn)
    {
        m_queue.push_back(std::move(fn));
    }
    bool pump()
    {
        if(m_queue.empty())
            return false;
        auto fn = std::move(m_queue.front());
        m_queue.pop_front();
        fn();
        return true;
    }

private:
    std::deque<plexus::detail::move_only_function<void()>> m_queue;
};

// A timer the Policy needs but the proof never fires: the three verbs + the two required ctors.
class uart_timer
{
public:
    explicit uart_timer(uart_executor &)
    {
    }
    uart_timer(uart_executor &ex, std::error_code &)
            : uart_timer(ex)
    {
    }
    void expires_after(std::chrono::milliseconds)
    {
    }
    void async_wait(plexus::detail::move_only_function<void(std::error_code)> cb)
    {
        m_handler_cb = std::move(cb);
    }
    void cancel()
    {
    }

private:
    plexus::detail::move_only_function<void(std::error_code)> m_handler_cb;
};

// The UART Policy: the step-executor by reference, the no-op channel, the inert timer, the
// shared_ptr byte_owner. post() forwards onto the executor.
struct uart_policy
{
    using executor_type     = uart_executor &;
    using byte_channel_type = uart_channel;
    using timer_type        = uart_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

// The single non-shm transport leaf: a serial connector whose verbs are inert.
class uart_transport
{
public:
    void listen(const pio::endpoint &)
    {
    }
    void dial(const pio::endpoint &)
    {
    }
    void close()
    {
    }
    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<uart_channel>)> cb)
    {
        m_on_accepted_cb = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<uart_channel>, const pio::endpoint &)> cb)
    {
        m_on_dialed_cb = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const pio::endpoint &, pio::io_error)> cb)
    {
        m_on_dial_failed_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(pio::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

private:
    plexus::detail::move_only_function<void(std::unique_ptr<uart_channel>)> m_on_accepted_cb;
    plexus::detail::move_only_function<void(std::unique_ptr<uart_channel>, const pio::endpoint &)> m_on_dialed_cb;
    plexus::detail::move_only_function<void(const pio::endpoint &, pio::io_error)> m_on_dial_failed_cb;
    plexus::detail::move_only_function<void(pio::io_error)> m_on_error_cb;
};

static_assert(plexus::Policy<uart_policy>, "uart_policy must satisfy Policy");
static_assert(pio::transport_backend<uart_transport, uart_policy>, "uart_transport must satisfy transport_backend");

}

int main()
{
    using node_t = plexus::node<uart_gate::uart_policy, uart_gate::uart_transport>;

    uart_gate::uart_executor ex;
    uart_gate::uart_transport transport;
    plexus::discovery::static_discovery disc{{}};

    plexus::node_id self{};
    self[0] = std::byte{0x01};

    plexus::node_options opts;
    node_t node{ex, disc, self, transport, opts};

    // Declares the topic through the opaque-geometry declare seam (a null const void* override
    // here): the transport-name-free publisher path compiles with no shm geometry type in scope.
    plexus::publisher<void> telemetry{node, "telemetry"};
    (void)telemetry;

    return 0;
}
