#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_TRANSPORT_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_TRANSPORT_H

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/run_task.h"
#include "plexus/freertos/freertos_executor.h"

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

// The constrained-target TCP connector. A node dials a peer: the transport constructs a
// stream_socket, connects it, and hands back ONE lwip_channel through on_dialed carrying the
// endpoint (the engine's correlation key — a completion routes to its slot by endpoint, never
// bleeding into another dial). Construction defaults to lwip_channel's MCU knob-down limits, never
// the PC ceilings. The Socket parameter is the injection seam: lwip_socket on hardware, the host's
// POSIX socket in the loopback slice.
//
// The server half (listen/accept) is present-but-deferred: the verbs exist so the transport_backend
// concept is satisfied, but a real acceptor is not built here — an inbound channel is delivered
// through on_accepted only once the accept path lands.
template<plexus::stream::stream_socket Socket>
class lwip_transport
{
public:
    using channel_type = lwip_channel<Socket>;

    explicit lwip_transport(freertos_executor &ex, std::size_t read_buffer = lwip_channel_limits::read_buffer_bytes, std::size_t max_message = lwip_channel_limits::max_message_bytes,
                            std::size_t reassembly_budget = lwip_channel_limits::reassembly_bytes, std::size_t egress_cap = lwip_channel_limits::egress_cap_bytes)
            : m_executor(ex)
            , m_channel(nullptr)
            , m_rx_task(std::nullopt)
            , m_read_buffer(read_buffer)
            , m_max_message(max_message)
            , m_reassembly_budget(reassembly_budget)
            , m_egress_cap(egress_cap)
    {
    }

    lwip_transport(const lwip_transport &)            = delete;
    lwip_transport &operator=(const lwip_transport &) = delete;

    // Opt into the P2 receive policy: a dedicated RX task (explicit stack) blocking-recvs into the
    // channel's pooled slots and posts the feed to the executor task, delivering on_data POSTED.
    // Unset, the transport stays on P1 — poll() drives a non-blocking recv with synchronous delivery,
    // and no RX task is spawned (don't pay for what you don't need).
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

    // The server half is not yet built — the accept path delivers an inbound channel through
    // on_accepted once the acceptor lands.
    void listen(const plexus::io::endpoint &)
    {
    }

    // The dialed endpoint rides back through on_dialed / on_dial_failed so the engine correlates
    // the completion to its slot by endpoint.
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

    // The cooperative RX step the super-loop drives once per iteration. The engine owns the
    // delivered channel (it moved the unique_ptr into a session), but the channel's poll —
    // socket.recv -> stream_inbound.feed -> on_data -> engine — is a same-task step the generic
    // engine does not arm (the host channels self-arm async reads). So the transport keeps a
    // NON-OWNING handle to the single channel it minted and forwards the poll; the handle stays
    // valid for the whole run (node, transport, and super-loop never return). The single handle is
    // the dial-only/single-connection shape; the accept generalization is a later phase.
    void poll()
    {
        if(m_channel && !m_rx_task)
            m_channel->poll();
    }

    void close()
    {
    }

private:
    // Mint the channel, record a non-owning handle for poll(), then hand ownership to the engine
    // through on_dialed. The raw handle never owns; capture it BEFORE the unique_ptr is moved out.
    // Under P2 the RX task is spawned here against the stable channel object (make_unique heap, the
    // pointer survives the unique_ptr move into the engine session — the task borrows by reference).
    std::unique_ptr<channel_type> adopt(Socket socket, const plexus::io::endpoint &ep)
    {
        auto ch   = std::make_unique<channel_type>(std::move(socket), m_executor, ep, m_read_buffer, m_max_message, m_reassembly_budget, m_egress_cap);
        m_channel = ch.get();
        if(m_rx_task)
            spawn_lwip_rx_task(*m_channel, m_executor, *m_rx_task);
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

    freertos_executor          &m_executor;
    channel_type               *m_channel; // non-owning poll handle; the engine owns the channel
    std::optional<task_options> m_rx_task; // set => P2 RX-task receive; unset => P1 poll drive
    std::size_t                 m_read_buffer;
    std::size_t        m_max_message;
    std::size_t        m_reassembly_budget;
    std::size_t        m_egress_cap;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)>                            m_on_accepted_cb;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const plexus::io::endpoint &)> m_on_dialed_cb;
    plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>        m_on_dial_failed_cb;
    plexus::detail::move_only_function<void(plexus::io::io_error)>                                     m_on_error_cb;
};

}

static_assert(plexus::io::transport_backend<plexus::freertos::lwip_transport<plexus::freertos::detail::null_stream_socket>, plexus::freertos::lwip_policy<plexus::freertos::detail::null_stream_socket>>,
              "lwip_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif
