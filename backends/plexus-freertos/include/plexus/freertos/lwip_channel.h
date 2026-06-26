#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_H

#include "plexus/freertos/detail/lwip_channel_egress.h"
#include "plexus/freertos/detail/lwip_channel_inbound.h"
#include "plexus/freertos/detail/null_stream_socket.h"
#include "plexus/freertos/detail/lwip_rx_slot_pool.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
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

// MCU knob-down defaults: an lwip_channel-LOCAL constant set, never a mutation of the shared
// fragmentation.h ceilings (the PC build is byte-identical). The read buffer matches the lwIP
// default TCP window (5760 B = 4x the 1440 MSS) so one recv drains a full advertised window; the
// ceilings sit well under the 64 KiB / 128 KiB / 16 KiB host defaults. The RX pool's SRAM is
// rx_slots * read_buffer_bytes (~22.5 KiB here), so both knobs are kept small.
struct lwip_channel_limits
{
    static constexpr std::size_t read_buffer_bytes = 5760;        // the lwIP TCP_WND default
    static constexpr std::size_t max_message_bytes = 8 * 1024;
    static constexpr std::size_t reassembly_bytes  = 16 * 1024;
    static constexpr std::size_t egress_cap_bytes  = 8 * 1024;
    static constexpr std::size_t rx_slots          = 4;
};

// The constrained-target TCP byte_channel: a byte_channel over a connected stream_socket that
// reuses the stream family VERBATIM — the PLAIN stream_inbound (no CRC decorator; TCP provides
// integrity, the CRC layer is UART-only) for framing and send_queue for egress. P1 poll-from-loop:
// a non-blocking recv() drained inside poll() on the executor task delivers on_data synchronously,
// driven only by the user's tick/run loop — no plexus-level thread. Fully-qualify
// plexus::detail::move_only_function below: an io/host-shim detail namespace in scope here shadows
// the bare detail:: lookup (the documented shadowing pitfall, see freertos_policy.h).
template<plexus::stream::stream_socket S>
class lwip_channel
{
public:
    // The RX hand-off pool holds k_rx_slots buffers and must stay within the executor queue depth, so
    // it never holds more in-flight slots than the queue can carry a post for.
    static constexpr std::size_t k_rx_slots = lwip_channel_limits::rx_slots;
    static_assert(k_rx_slots <= freertos_executor::k_queue_depth, "the RX pool must not outsize the executor queue depth");
    using rx_slot = detail::rx_slot<lwip_channel, lwip_channel_limits::read_buffer_bytes>;

    lwip_channel(S socket, freertos_executor &ex, plexus::io::endpoint remote, std::size_t read_buffer = lwip_channel_limits::read_buffer_bytes,
                 std::size_t max_message = lwip_channel_limits::max_message_bytes, std::size_t reassembly_budget = lwip_channel_limits::reassembly_bytes,
                 std::size_t egress_cap = lwip_channel_limits::egress_cap_bytes, plexus::io::congestion congestion = plexus::io::congestion::block)
            : m_socket(std::move(socket))
            , m_remote(std::move(remote))
            , m_read_buffer(read_buffer)
            , m_inbound(ex, plexus::stream::with_message_limits({}, max_message, reassembly_budget))
            , m_egress(m_socket, m_on_error, egress_cap, congestion)
            , m_rx_pool(*this)
    {
        wire_inbound();
    }

    rx_slot *acquire_rx_slot(TickType_t wait) noexcept
    {
        return m_rx_pool.acquire(wait);
    }
    void release_rx_slot(rx_slot &slot) noexcept
    {
        m_rx_pool.release(slot);
    }

    void send(std::span<const std::byte> framed)
    {
        m_egress.send(framed);
    }

    void close()
    {
        m_inbound.shutdown();
        m_egress.close();
        m_socket.close();
        if(m_on_closed)
            m_on_closed();
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return m_remote;
    }
    bool closed() const noexcept
    {
        return m_socket.closed();
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
        m_inbound.feed(std::span<const std::byte>{slot.buffer.data(), slot.len});
        release_rx_slot(slot);
    }

    static void invoke_feed(void *ctx) noexcept
    {
        auto &slot = *static_cast<rx_slot *>(ctx);
        slot.owner->feed_from_slot(slot);
    }

private:
    // The shared recv + hard-drop classification both receive paths run: a recv into the caller's
    // buffer, returning the byte count; a closed socket fires on_error on the connection-reset seam.
    template<typename Buffer>
    std::size_t recv_step(Buffer &buffer)
    {
        const std::size_t n = m_socket.recv(buffer);
        if(n == 0 && m_socket.closed() && m_on_error)
            m_on_error(plexus::io::io_error::connection_reset);
        return n;
    }

    void wire_inbound()
    {
        detail::lwip_wire_inbound(m_inbound, m_on_data, m_on_protocol_close);
    }

    // m_on_error precedes m_egress: the egress borrows it by reference (hard send drop + block
    // backpressure surface through it), so it must be constructed first.
    detail::on_data_cb                                                 m_on_data;
    plexus::detail::move_only_function<void()>                         m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>     m_on_error;
    detail::on_protocol_close_cb                                       m_on_protocol_close;
    S                                                                   m_socket;
    plexus::io::endpoint                                                m_remote;
    std::vector<std::byte>                                              m_read_buffer;
    plexus::stream::stream_inbound<freertos_timer, freertos_executor &> m_inbound;
    detail::lwip_channel_egress<S>                                      m_egress;
    detail::lwip_rx_slot_pool<lwip_channel, k_rx_slots, lwip_channel_limits::read_buffer_bytes> m_rx_pool;
};

static_assert(plexus::io::byte_channel<lwip_channel<detail::null_stream_socket>>, "lwip_channel must satisfy byte_channel — check the seven verbs");

}

#endif
