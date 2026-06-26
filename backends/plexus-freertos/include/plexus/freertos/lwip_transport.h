#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_TRANSPORT_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_TRANSPORT_H

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/run_task.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/null_acceptor.h"
#include "plexus/freertos/detail/lwip_channel_views.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <cstddef>
#include <utility>
#include <optional>
#include <system_error>

namespace plexus::freertos {

// The constrained-target TCP transport: a node dials a peer (the transport connects a stream_socket
// and hands back ONE lwip_channel through on_dialed carrying the endpoint correlation key) AND,
// when an Acceptor is named, listens — each accepted connection is minted into a channel delivered
// through on_accepted. Construction defaults to lwip_channel's MCU knob-down limits.
//
// MaxClients is the COMPILE-TIME accept cap (default 1 — the typical MCU single-client role): poll()
// is poll-all over a BOUNDED, no-heap set of non-owning channel views; a connection over the cap is
// refused. For N=1 the view set collapses to the single-slot case the dial-only path compiles to.
// The Socket and Acceptor are the injection seams: lwip_socket/lwip_acceptor on hardware, the host's
// POSIX socket/listener in the loopback slice. The default null_acceptor never listens (dial-only).
template<plexus::stream::stream_socket Socket, typename Acceptor = detail::null_acceptor<Socket>, std::size_t MaxClients = 1>
class lwip_transport
{
public:
    using channel_type = lwip_channel<Socket>;

    explicit lwip_transport(freertos_executor &ex, std::size_t read_buffer = lwip_channel_limits::read_buffer_bytes, std::size_t max_message = lwip_channel_limits::max_message_bytes,
                            std::size_t reassembly_budget = lwip_channel_limits::reassembly_bytes, std::size_t egress_cap = lwip_channel_limits::egress_cap_bytes)
            : m_executor(ex)
            , m_acceptor()
            , m_views()
            , m_rx_task(std::nullopt)
            , m_listening(false)
            , m_read_buffer(read_buffer)
            , m_max_message(max_message)
            , m_reassembly_budget(reassembly_budget)
            , m_egress_cap(egress_cap)
    {
    }

    lwip_transport(const lwip_transport &)            = delete;
    lwip_transport &operator=(const lwip_transport &) = delete;

    // Opt into the P2 receive policy: a dedicated RX task blocking-recvs into the channel's pooled
    // slots and posts the feed to the executor task, delivering on_data POSTED. Unset, the transport
    // stays on P1 — poll() drives a non-blocking recv with synchronous delivery, no RX task spawned.
    void use_rx_task(task_options topts)
    {
        m_rx_task = topts;
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
        m_on_dial_failed_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void listen(const plexus::io::endpoint &ep)
    {
        m_listening = !m_acceptor.bind_and_listen(ep);
    }

    // The dialed endpoint rides back through on_dialed / on_dial_failed so the engine correlates the
    // completion to its slot by endpoint. The dialed channel's view joins the set too, so poll-all
    // and the P2 RX path are uniform across dial and accept.
    void dial(const plexus::io::endpoint &ep)
    {
        Socket socket;
        if(std::error_code ec = socket.connect(ep); ec)
            return report_dial_fail(ep, map_error(ec));
        if(m_rx_task)
            socket.set_blocking(true);
        if(m_on_dialed_cb)
            m_on_dialed_cb(adopt(std::move(socket), ep), ep);
    }

    // The cooperative RX step the super-loop drives: drain the acceptor, then poll-all the live
    // views (P1 only — P2 receives on the RX task). The transport keeps NON-OWNING views to the
    // engine-owned channels; each is cleared on close, so poll-all never touches a dangling view.
    void poll()
    {
        if(m_listening)
            accept_pending();
        if(!m_rx_task)
            m_views.poll_each([](channel_type &ch) { ch.poll(); });
    }

    void close()
    {
        m_acceptor.close();
        m_listening = false;
    }

private:
    // Mint an accepted channel and hand ownership to the engine through on_accepted — refused (the
    // socket dropped, the set never grows) when the cap is reached, the bound enforced by construction.
    void accept_pending()
    {
        std::optional<Socket> socket = m_acceptor.accept_one();
        if(!socket)
            return;
        if(m_views.full() || !m_on_accepted_cb)
            return;
        if(m_rx_task)
            socket->set_blocking(true);
        m_on_accepted_cb(adopt(std::move(*socket), plexus::io::endpoint{"tcp", {}}));
    }

    // Mint the channel, record its non-owning view, and ARM the on_closed view-invalidation hook
    // BEFORE returning — so by the time the caller fires on_dialed/on_accepted (ownership moves to
    // the engine) and the RX task is spawned, the hook is already wired: no window where the engine
    // owns the channel but the transport's poll-view is uninstrumented. on_closed clears the view
    // first if the channel closes the instant after hand-off.
    std::unique_ptr<channel_type> adopt(Socket socket, const plexus::io::endpoint &ep)
    {
        auto ch          = std::make_unique<channel_type>(std::move(socket), m_executor, ep, m_read_buffer, m_max_message, m_reassembly_budget, m_egress_cap);
        channel_type *view = ch.get();
        m_views.add(view);
        ch->on_closed([this, view] { m_views.remove(view); });
        if(m_rx_task)
            spawn_lwip_rx_task(*view, m_executor, *m_rx_task);
        return ch;
    }

    static plexus::io::io_error map_error(std::error_code ec)
    {
        if(ec == std::errc::connection_refused)
            return plexus::io::io_error::connection_refused;
        if(ec == std::errc::timed_out)
            return plexus::io::io_error::timed_out;
        if(ec == std::errc::address_in_use)
            return plexus::io::io_error::address_in_use;
        return plexus::io::io_error::unknown;
    }

    void report_dial_fail(const plexus::io::endpoint &ep, plexus::io::io_error e)
    {
        if(m_on_dial_failed_cb)
            m_on_dial_failed_cb(ep, e);
    }

    freertos_executor                                     &m_executor;
    Acceptor                                               m_acceptor;
    detail::lwip_channel_views<channel_type, MaxClients>  m_views;
    std::optional<task_options>                           m_rx_task;
    bool                                                  m_listening;
    std::size_t                                           m_read_buffer;
    std::size_t                                           m_max_message;
    std::size_t                                           m_reassembly_budget;
    std::size_t                                           m_egress_cap;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)>                               m_on_accepted_cb;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const plexus::io::endpoint &)> m_on_dialed_cb;
    plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>          m_on_dial_failed_cb;
    plexus::detail::move_only_function<void(plexus::io::io_error)>                                        m_on_error_cb;
};

}

static_assert(plexus::io::transport_backend<plexus::freertos::lwip_transport<plexus::freertos::detail::null_stream_socket>, plexus::freertos::lwip_policy<plexus::freertos::detail::null_stream_socket>>,
              "lwip_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
