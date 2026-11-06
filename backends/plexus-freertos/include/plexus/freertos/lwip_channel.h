#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_H

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/lwip_channel_limits.h"

#include "plexus/freertos/detail/close_state.h"
#include "plexus/freertos/detail/lwip_rx_lifecycle.h"
#include "plexus/freertos/detail/null_stream_socket.h"
#include "plexus/freertos/detail/lwip_channel_egress.h"
#include "plexus/freertos/detail/lwip_channel_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"

#include "plexus/stream/stream_socket.h"
#include "plexus/stream/stream_inbound.h"

#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::freertos {

// The constrained-target TCP byte_channel over a connected stream_socket, framed by the plain stream_inbound (TCP
// provides integrity, so no CRC layer). P1 polls from the loop: a non-blocking recv() inside poll() on the executor
// task delivers on_data synchronously. plexus::detail is spelled in full: an io detail namespace in scope here shadows
// the bare detail:: lookup.
template<plexus::stream::stream_socket S>
class lwip_channel
{
public:
    using rx_type                           = detail::lwip_rx_lifecycle<lwip_channel, S, lwip_channel_limits::rx_slots, lwip_channel_limits::read_buffer_bytes>;
    using rx_slot                           = typename rx_type::slot_type;
    static constexpr std::size_t k_rx_slots = rx_type::k_slots;

    lwip_channel(S socket, freertos_executor &ex, plexus::io::endpoint remote, std::size_t read_buffer = lwip_channel_limits::read_buffer_bytes,
                 std::size_t max_message = lwip_channel_limits::max_message_bytes, std::size_t reassembly_budget = lwip_channel_limits::reassembly_bytes,
                 std::size_t egress_cap = lwip_channel_limits::egress_cap_bytes, plexus::io::congestion congestion = plexus::io::congestion::block)
            : m_close(ex)
            , m_socket(std::move(socket))
            , m_remote(std::move(remote))
            , m_read_buffer(read_buffer)
            , m_inbound(ex, plexus::stream::with_message_limits({}, max_message, reassembly_budget))
            , m_egress(
                      m_socket, m_on_error, [this](plexus::io::io_error e) { fail(e); }, egress_cap, congestion)
            , m_rx(*this, m_socket, m_close, ex)
    {
        wire_inbound();
    }

    rx_type &rx() noexcept
    {
        return m_rx;
    }

    void send(std::span<const std::byte> framed)
    {
        if(!m_close.closed())
            m_egress.send(framed);
    }

    void close()
    {
        if(!m_close.enter())
            return;
        m_inbound.shutdown();
        m_egress.close();
        if(m_rx.live())
            m_rx.request_exit();
        else
            m_socket.close();
        m_close.shut_down();
        m_on_data           = nullptr;
        m_on_error          = nullptr;
        m_on_protocol_close = nullptr;
        m_close.notify();
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return m_remote;
    }
    bool closed() const noexcept
    {
        return m_close.closed();
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_close.install(std::move(cb));
    }
    void on_shutdown(plexus::detail::move_only_function<void()> cb)
    {
        m_close.on_shutdown(std::move(cb));
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }

    // A hard connection drop fails the channel through on_error, a seam distinct from on_protocol_close.
    void poll()
    {
        m_egress.poll_egress(); // retry a soft-stalled gather (the send_queue won't re-issue it itself)
        const std::size_t n = recv_step(m_read_buffer);
        if(n > 0)
            m_inbound.feed(std::span<const std::byte>{m_read_buffer.data(), n});
    }

    // The egress congestion drop count (drop_newest sheds + counts here); zero under block.
    std::size_t dropped() const noexcept
    {
        return m_egress.dropped();
    }

    // P2 RX-task side: one recv into the slot's pooled buffer (blocking on-target), recording the
    // count WITHOUT feeding — the RX task owns the post, the channel owns framing + the slot lifecycle.
    std::size_t recv_into_slot(rx_slot &slot) noexcept
    {
        slot.len = recv_step(slot.buffer);
        return slot.len;
    }

    // P2's posted-delivery contract: feed the pooled slot's bytes on the EXECUTOR task (so on_frame
    // -> deliver -> on_data fire POSTED, the cross-context pair to P1's synchronous divergence), then
    // release the slot back to the fixed pool. No per-recv heap — the bytes lived in pool storage.
    void feed_from_slot(rx_slot &slot)
    {
        if(!m_close.closed())
            m_inbound.feed(std::span<const std::byte>{slot.buffer.data(), slot.len});
        m_rx.release(slot);
    }

private:
    // The shared recv and hard-drop classification both receive paths run.
    template<typename Buffer>
    std::size_t recv_step(Buffer &buffer)
    {
        const std::size_t n = m_socket.recv(buffer);
        if(n == 0 && m_socket.closed())
            fail(plexus::io::io_error::connection_reset);
        return n;
    }

    // The error handler is taken before the close detaches it and runs last, since it may tear the channel's owner down.
    void fail(plexus::io::io_error e)
    {
        auto on_error = std::exchange(m_on_error, nullptr);
        close();
        if(on_error)
            on_error(e);
    }

    void wire_inbound()
    {
        detail::lwip_wire_inbound(m_inbound, m_on_data, m_on_protocol_close, [this] { close(); });
    }

    // m_on_error precedes m_egress: the egress borrows it by reference (block backpressure surfaces
    // through it), so it must be constructed first.
    detail::on_data_cb m_on_data;
    detail::close_state m_close;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error;
    detail::on_protocol_close_cb m_on_protocol_close;
    S m_socket;
    plexus::io::endpoint m_remote;
    std::vector<std::byte> m_read_buffer;
    plexus::stream::stream_inbound<freertos_timer, freertos_executor &> m_inbound;
    detail::lwip_channel_egress<S> m_egress;
    rx_type m_rx;
};

static_assert(plexus::io::byte_channel<lwip_channel<detail::null_stream_socket>>, "lwip_channel must satisfy byte_channel — check the seven verbs");

}

#endif
