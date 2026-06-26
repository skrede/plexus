#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_H

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/byte_channel.h"
#include "plexus/stream/stream_socket.h"
#include "plexus/stream/stream_inbound.h"
#include "plexus/stream/detail/send_queue.h"

#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::freertos {

// MCU knob-down defaults: an lwip_channel-LOCAL constant set, never a mutation of the shared
// fragmentation.h ceilings (the PC build is byte-identical). The read buffer sits near the lwIP
// TCP window (a few KiB, NOT the host's 64 KiB), and the message/reassembly/egress ceilings are
// dialed well under the 4 MiB / 16 MiB host defaults. These are pre-empirical starting values:
// tuned on hardware against the SRAM budget, per the empirical-sweep discipline.
struct lwip_channel_limits
{
    static constexpr std::size_t read_buffer_bytes = 4 * 1024;
    static constexpr std::size_t max_message_bytes = 64 * 1024;
    static constexpr std::size_t reassembly_bytes  = 128 * 1024;
    static constexpr std::size_t egress_cap_bytes  = 16 * 1024;
};

// The constrained-target TCP byte_channel: a byte_channel over a connected stream_socket that
// reuses the stream family VERBATIM — the PLAIN stream_inbound (no CRC decorator; TCP provides
// integrity, the CRC layer is UART-only) for framing and send_queue for egress. P1 poll-from-
// loop: a non-blocking recv() drained inside poll() on the executor task delivers on_data
// synchronously, driven only by the user's tick/run loop — no plexus-level thread.
//
// Fully-qualify plexus::detail::move_only_function below: an io/host-shim detail namespace is in
// scope inside plexus::freertos and would shadow the bare detail:: lookup (the documented
// move_only_function shadowing pitfall, see freertos_policy.h).
template<plexus::stream::stream_socket S>
class lwip_channel
{
public:
    lwip_channel(S socket, freertos_executor &ex, plexus::io::endpoint remote, std::size_t read_buffer = lwip_channel_limits::read_buffer_bytes,
                 std::size_t max_message = lwip_channel_limits::max_message_bytes, std::size_t reassembly_budget = lwip_channel_limits::reassembly_bytes,
                 std::size_t egress_cap = lwip_channel_limits::egress_cap_bytes)
            : m_socket(std::move(socket))
            , m_remote(std::move(remote))
            , m_read_buffer(read_buffer)
            , m_inbound(ex, plexus::stream::with_message_limits({}, max_message, reassembly_budget))
            , m_send_queue([this](plexus::stream::detail::send_queue::buffer_sequence views, plexus::stream::detail::send_queue::completion done) { drain_views(views, std::move(done)); }, egress_cap)
    {
        wire_inbound();
    }

    void send(std::span<const std::byte> framed)
    {
        m_send_queue.enqueue(framed);
    }

    void close()
    {
        m_inbound.shutdown();
        m_send_queue.close_and_drain();
        m_socket.close();
        if(m_on_closed)
            m_on_closed();
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return m_remote;
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }

    // One cooperative super-loop step: a non-blocking recv into the fixed read buffer, fed to the
    // reused stream_inbound. A hard connection drop (the socket's closed predicate) fires on_error
    // on a seam DISTINCT from on_protocol_close (the framing-violation seam stream_inbound owns).
    void poll()
    {
        const std::size_t n = m_socket.recv(m_read_buffer);
        if(n > 0)
            m_inbound.feed(std::span<const std::byte>{m_read_buffer.data(), n});
        else if(m_socket.closed() && m_on_error)
            m_on_error(plexus::io::io_error::connection_reset);
    }

private:
    void wire_inbound()
    {
        m_inbound.on_frame([this](const plexus::wire::complete_frame &f) { deliver(static_cast<std::span<const std::byte>>(f.payload)); });
        m_inbound.on_protocol_close(
                [this](plexus::wire::close_cause cause)
                {
                    if(m_on_protocol_close)
                        m_on_protocol_close(cause);
                });
    }

    // The P1 divergence from the always-posted byte_channel contract, sound the same way the UART
    // path is: poll() runs on the executor task and the framed span is consumed FULLY before poll()
    // returns, so a synchronous on_data re-enters nothing the loop has not already finished — the
    // re-entrancy invariant holds. It deliberately avoids the per-frame owning heap copy a posted
    // delivery needs, which is banned on the -fno-exceptions target. Do NOT "restore" the post.
    void deliver(std::span<const std::byte> frame)
    {
        if(m_on_data)
            m_on_data(frame);
    }

    // lwIP has no scatter writev, so the gathered views are sent one by one; a transient short
    // send is local congestion the socket already folded to 0 (the queue re-arms), never a tear-
    // down. Completion fires once the gather has been handed to the socket.
    void drain_views(plexus::stream::detail::send_queue::buffer_sequence views, plexus::stream::detail::send_queue::completion done)
    {
        for(const auto &v : views)
            m_socket.send(v);
        done(true);
    }

    S                                                                   m_socket;
    plexus::io::endpoint                                                m_remote;
    std::vector<std::byte>                                              m_read_buffer;
    plexus::stream::stream_inbound<freertos_timer, freertos_executor &> m_inbound;
    plexus::stream::detail::send_queue                                  m_send_queue;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>       m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)>  m_on_protocol_close;
};

namespace detail {

// The minimal conforming witness for the in-header byte_channel proof: a do-nothing stream_socket
// so the static_assert below is self-contained (the real sockets are the lwIP one and the host's
// POSIX one). It models the seam, nothing more.
struct null_stream_socket
{
    using endpoint_type = plexus::io::endpoint;

    std::error_code connect(endpoint_type)
    {
        return {};
    }
    std::size_t send(std::span<const std::byte>)
    {
        return 0;
    }
    std::size_t recv(std::span<std::byte>)
    {
        return 0;
    }
    bool closed() const
    {
        return false;
    }
    void close()
    {
    }
};

}

static_assert(plexus::io::byte_channel<lwip_channel<detail::null_stream_socket>>, "lwip_channel must satisfy byte_channel — check the seven verbs");

}

#endif
