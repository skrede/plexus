#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_TRANSPORT_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_TRANSPORT_H

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/lwip_dialer.h"
#include "plexus/freertos/detail/null_acceptor.h"
#include "plexus/freertos/detail/lwip_channel_views.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/transport_backend.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <memory>
#include <cstddef>
#include <utility>
#include <optional>

namespace plexus::freertos {

// MaxClients is the compile-time channel cap dialed and accepted channels share (default 1, the typical MCU client):
// poll() is poll-all over a bounded, no-heap set of non-owning channel views, and a connection over the cap is refused.
// The Socket and Acceptor are the injection seams: lwip_socket/lwip_acceptor on hardware, the host's POSIX
// socket/listener under test. The default null_acceptor never listens.
template<plexus::stream::stream_socket Socket, typename Acceptor = detail::null_acceptor<Socket>, std::size_t MaxClients = 1>
class lwip_transport
{
public:
    using channel_type = lwip_channel<Socket>;

    explicit lwip_transport(freertos_executor &ex, std::size_t read_buffer = lwip_channel_limits::read_buffer_bytes, std::size_t max_message = lwip_channel_limits::max_message_bytes,
                            std::size_t reassembly_budget = lwip_channel_limits::reassembly_bytes, std::size_t egress_cap = lwip_channel_limits::egress_cap_bytes,
                            plexus::io::congestion congestion = plexus::io::congestion::block)
            : m_executor(ex)
            , m_acceptor()
            , m_views()
            , m_dialer(ex)
            , m_rx_task(std::nullopt)
            , m_listening(false)
            , m_read_buffer(read_buffer)
            , m_max_message(max_message)
            , m_reassembly_budget(reassembly_budget)
            , m_egress_cap(egress_cap)
            , m_congestion(congestion)
    {
    }

    lwip_transport(const lwip_transport &)            = delete;
    lwip_transport &operator=(const lwip_transport &) = delete;

    // Applies to channels adopted after the call. The task receives while the executor sends on the same socket, which
    // needs lwIP's LWIP_NETCONN_FULLDUPLEX (ESP-IDF enables it). A channel's close then completes when its task exits, so
    // an owner pumps the executor until on_closed has run (for a node, the node's on_closed) before destroying it.
    void use_rx_task(rx_task_options opts)
    {
        detail::receive_timeout_ticks(opts.recv_timeout);
        m_rx_task = opts;
    }

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_on_accepted_cb = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const plexus::io::endpoint &)> cb)
    {
        m_on_dialed_cb = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)> cb)
    {
        m_dialer.on_failed(std::move(cb));
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void listen(const plexus::io::endpoint &ep)
    {
        m_listening = !m_acceptor.bind_and_listen(ep);
    }

    // Applies to later dials; zero leaves a dial to lwIP's SYN retransmission limit.
    void set_dial_timeout(std::chrono::nanoseconds timeout) noexcept
    {
        m_dialer.set_timeout(timeout);
    }

    // A dial while the view set is full is refused off the call without opening a socket. Otherwise the dial completes
    // from poll(), or fails there with timed_out once its dial timeout elapses.
    void dial(const plexus::io::endpoint &ep)
    {
        if(m_views.full())
            return m_dialer.refuse(ep, plexus::io::io_error::would_block);
        m_dialer.dial(ep);
    }

    // The views are non-owning and cleared on close, so poll-all never touches a dangling one. Every view is polled under
    // both policies, and a channel its receive task serves only retries its stalled send there.
    void poll()
    {
        if(m_listening)
            accept_pending();
        m_dialer.complete_each([this](Socket socket, const plexus::io::endpoint &ep) { deliver_dialed(std::move(socket), ep); });
        m_views.poll_each([](channel_type &ch) { ch.poll(); });
    }

    // The shed total folded over the LIVE views only (a closed channel drops out of the set).
    std::size_t dropped()
    {
        std::size_t total = 0;
        m_views.poll_each([&total](channel_type &ch) { total += ch.dropped(); });
        return total;
    }

    void close()
    {
        m_dialer.abort_all();
        m_acceptor.close();
        m_listening = false;
    }

private:
    // Mint an accepted channel and hand ownership to the engine through on_accepted — refused (the socket dropped, the
    // set never grows) when the cap is reached, the bound enforced by construction, or when adopt cannot set it up.
    void accept_pending()
    {
        std::optional<Socket> socket = m_acceptor.accept_one();
        if(!socket)
            return;
        if(m_views.full() || !m_on_accepted_cb)
            return;
        if(auto ch = adopt(std::move(*socket), plexus::io::endpoint{"tcp", {}}))
            m_on_accepted_cb(std::move(ch));
    }

    // The set can still fill while a connect is pending (an accept can fill it). A dialed channel past a full set
    // would never be polled, so the dial fails and its socket closes, as it does when adopt cannot set the channel up.
    void deliver_dialed(Socket socket, const plexus::io::endpoint &ep)
    {
        if(m_views.full())
            return m_dialer.report(ep, plexus::io::io_error::would_block);
        if(!m_on_dialed_cb)
            return;
        auto ch = adopt(std::move(socket), ep);
        if(!ch)
            return m_dialer.report(ep, plexus::io::io_error::unknown);
        m_on_dialed_cb(std::move(ch), ep);
    }

    // The view-invalidation hook is armed before the channel is handed out, so no window exists where the engine owns
    // the channel but its poll view is uninstrumented. It rides the channel's shutdown hook, synchronously at close,
    // because on_closed belongs to the owner, which retires the channel through it. Under the receive-task policy a
    // receive timeout the socket rejects or a task that cannot be created yields no channel, and the socket closes.
    std::unique_ptr<channel_type> adopt(Socket socket, const plexus::io::endpoint &ep)
    {
        if(m_rx_task)
        {
            socket.set_blocking(true);
            if(!socket.set_receive_timeout(m_rx_task->recv_timeout))
                return nullptr;
        }
        auto ch = std::make_unique<channel_type>(std::move(socket), m_executor, ep, m_read_buffer, m_max_message, m_reassembly_budget, m_egress_cap, m_congestion);
        if(m_rx_task && spawn_lwip_rx_task(*ch, m_executor, *m_rx_task) != pdTRUE)
            return nullptr;
        channel_type *view = ch.get();
        m_views.add(view);
        ch->on_shutdown([this, view] { m_views.remove(view); });
        return ch;
    }

    freertos_executor &m_executor;
    Acceptor m_acceptor;
    detail::lwip_channel_views<channel_type, MaxClients> m_views;
    detail::lwip_dialer<Socket, MaxClients> m_dialer;
    std::optional<rx_task_options> m_rx_task;
    bool m_listening;
    std::size_t m_read_buffer;
    std::size_t m_max_message;
    std::size_t m_reassembly_budget;
    std::size_t m_egress_cap;
    plexus::io::congestion m_congestion;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> m_on_accepted_cb;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const plexus::io::endpoint &)> m_on_dialed_cb;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error_cb;
};

}

static_assert(plexus::io::transport_backend<plexus::freertos::lwip_transport<plexus::freertos::detail::null_stream_socket>,
                                            plexus::freertos::lwip_policy<plexus::freertos::detail::null_stream_socket>>,
              "lwip_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
