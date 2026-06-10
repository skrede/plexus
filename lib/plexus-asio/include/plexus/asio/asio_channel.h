#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/stream_send_queue.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/write.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// TCP byte_channel over ::asio::ip::tcp::socket. Inbound bytes feed a member
// wire::stream_inbound (which composes the frame_reassembler and owns the
// no-progress slowloris timer); each complete frame is POSTED to on_data (per the
// byte_channel contract), while a framing violation or a no-progress stall raises
// on_protocol_close — a seam DISTINCT from on_error so the session discriminates
// peer-misbehaved (no re-dial) from network-dropped (re-dial). The channel is
// caller-owned and runs its read loop with `this` captured. The
// wire::stream_inbound_config (the node's hardening config the transport stamps
// every channel with) is a required-WITH-default ctor argument, defaulted to {} so
// asio_channel(io) mints a real default-config channel, never an unarmed one.
class asio_channel
{
public:
    // The bounded congestion=block write-queue BYTE budget (allocated at setup, never
    // grown on the hot path): a producer that outruns the socket drain back-pressures (or
    // sheds, under drop_newest) at a bounded cap instead of growing the userspace write deque to
    // the 10+ GB OOM the unbounded queue left possible. Sized at 1x the 4 MiB max-message
    // ceiling so this per-connection FIFO is the SHALLOW socket-facing buffer holding
    // roughly one frame in flight: the deep, priority-ordered backlog now lives in the
    // forwarder's egress bands ABOVE this queue, so a deep channel FIFO would silently
    // defeat banding by re-accumulating an un-prioritized backlog. This shallow cap is a
    // load-bearing knob — to be substantiated at the fan-out benchmark, not fixed by feel.
    static constexpr std::size_t default_write_queue_bytes =
        1u * io::fragmentation_limits::max_message_size;

    // Dial/executor-alone: unconnected, not reading yet — dial() calls start_read().
    // The congestion mode + byte budget are the per-channel QoS choice (block = the safe
    // reliable default that back-pressures; drop_newest = the opt-out shed), threaded as
    // required-WITH-default ctor args exactly as udp_channel threads io::congestion.
    explicit asio_channel(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                          io::congestion congestion = io::congestion::block,
                          std::size_t write_queue_bytes = default_write_queue_bytes)
        : m_io(io)
        , m_socket(io)
        , m_inbound(io, cfg)
        , m_congestion(congestion)
        , m_egress(make_send_sink(), write_queue_bytes)
    {
        wire_inbound();
    }

    // Accept-mode: adopt an already-connected socket and start reading.
    asio_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected,
                 wire::stream_inbound_config cfg = {},
                 io::congestion congestion = io::congestion::block,
                 std::size_t write_queue_bytes = default_write_queue_bytes)
        : m_io(io)
        , m_socket(std::move(connected))
        , m_inbound(io, cfg)
        , m_congestion(congestion)
        , m_egress(make_send_sink(), write_queue_bytes)
    {
        wire_inbound();
        m_open = true;
        do_read();
    }

    // The dtor only tears the socket/timer down — it never posts on_closed (a
    // this-capturing post could outlive the channel). close() posts on_closed.
    ~asio_channel() { m_inbound.shutdown(); shutdown_socket(); }

    asio_channel(const asio_channel &) = delete;
    asio_channel &operator=(const asio_channel &) = delete;
    asio_channel(asio_channel &&) = delete;
    asio_channel &operator=(asio_channel &&) = delete;

    // Enqueue for the serial async_write egress, bounded by the byte budget. A frame that
    // would carry the queued total past the cap is shed at the publisher under
    // congestion=drop_newest (counted for the observer), or refused under congestion=block with
    // the would_block stall edge surfaced (never an unbounded deque) — the same QoS edge
    // udp_channel threads. Compare-before-add so the running total never wraps.
    void send(std::span<const std::byte> data)
    {
        if(!m_open)
            return;
        if(!m_egress.enqueue(data))
            on_write_queue_full();
    }

    void close()
    {
        if(!m_socket.is_open())
            return;
        m_inbound.shutdown();
        shutdown_socket();
        m_egress.close();   // an aborted in-flight write must not chain onto the closed socket
        ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });   // posted, never synchronous
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        std::error_code ec;
        auto ep = m_socket.remote_endpoint(ec);
        if(ec)
            return {"tcp", ""};
        return {"tcp", ep.address().to_string() + ":" + std::to_string(ep.port())};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    [[nodiscard]] ::asio::ip::tcp::socket &socket() noexcept { return m_socket; }
    void start_read() { m_open = true; do_read(); }

    [[nodiscard]] io::congestion congestion_mode() const noexcept { return m_congestion; }
    // The count of frames shed under congestion=drop_newest (the drop-observer's edge).
    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_dropped; }
    // The current queued (un-drained) write-queue byte occupancy; 0 when the socket drains.
    [[nodiscard]] std::size_t backpressured() const noexcept { return m_egress.queued_bytes(); }

private:
    // The per-connection congestion safety net: it guards the DIRECT-send bypass paths
    // (latch replay, control frames, fetch_latched) and a misconfigured low-water gate, at
    // a granularity distinct from the forwarder's per-band overflow (no double-application).
    // congestion=drop_newest sheds the frame at the publisher (the documented opt-out of
    // the reliable guarantee); congestion=block surfaces would_block (the stall edge —
    // bounded, never unbounded growth). Either way the call returns without blocking.
    void on_write_queue_full()
    {
        if(m_congestion == io::congestion::drop_newest)
        {
            ++m_dropped;
            return;
        }
        if(m_on_error)
            m_on_error(io::io_error::would_block);
    }

    // Wire stream_inbound's two outputs to the channel's frame + close paths.
    void wire_inbound()
    {
        m_inbound.on_frame([this](const wire::complete_frame &f) { post_frame(f); });
        m_inbound.on_protocol_close([this](wire::close_cause c) { handle_protocol_close(c); });
    }

    // A peer that misbehaved on the wire: fire the distinct protocol-close seam, then
    // tear down via the on_closed-only close() path — NEVER fail()/on_error.
    void handle_protocol_close(wire::close_cause cause)
    {
        if(m_on_protocol_close)
            m_on_protocol_close(cause);
        close();
    }

    void shutdown_socket()
    {
        if(!m_socket.is_open())
            return;
        std::error_code ec;
        (void)m_socket.shutdown(::asio::ip::tcp::socket::shutdown_both, ec);
        (void)m_socket.close(ec);
        m_open = false;
    }

    void do_read()
    {
        m_socket.async_read_some(::asio::buffer(m_read_buf),
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return fail(ec);
                feed_inbound(std::span<const std::byte>{m_read_buf.data(), n});
                if(m_open)   // a protocol-close may have torn the socket down
                    do_read();
            });
    }

    void feed_inbound(std::span<const std::byte> bytes) { m_inbound.feed(bytes); }

    // The stream_inbound on_frame target: re-frame the reassembled frame into a
    // COMPLETE header-on frame and post THAT, so on_data delivers a full frame
    // (identical to the inproc channel) and the frame_router sees frame_header.type.
    // The re-frame uses a reused member scratch (no per-frame alloc); the owning
    // vector keeps the bytes alive across the post.
    void post_frame(const wire::complete_frame &frame)
    {
        wire::encode_frame_into(m_frame_scratch, frame.header, frame.payload);
        auto owned = std::make_shared<std::vector<std::byte>>(m_frame_scratch);
        ::asio::post(m_io, [this, owned]
        {
            if(m_on_data)
                m_on_data(std::span<const std::byte>{*owned});
        });
    }

    // The irreducible asio send-sink the stream_send_queue block drives: write the
    // block-owned node's bytes and signal completion when the async op finishes. At most
    // one is outstanding (the block's serial discipline). On a socket error the channel
    // fails (which closes the block), so the completion's open-guard stops the chain — the
    // exact fail-before-chain edge the hand-rolled do_write carried, never a swallow.
    io::detail::stream_send_queue::send_sink make_send_sink()
    {
        return [this](std::span<const std::byte> bytes, io::detail::stream_send_queue::completion done)
        {
            ::asio::async_write(m_socket, ::asio::buffer(bytes.data(), bytes.size()),
                [this, done = std::move(done)](std::error_code ec, std::size_t) mutable
                {
                    if(ec)
                        fail(ec);
                    done(!ec);
                });
        };
    }

    void fail(const std::error_code &ec)
    {
        if(ec == ::asio::error::operation_aborted || !m_open)
            return;
        m_open = false;
        m_egress.close();   // stop the serial drain so the failed write does not chain
        m_inbound.shutdown();
        auto mapped = detail::map_error(ec);
        if(m_on_error)
            m_on_error(mapped);
        if(m_on_closed)
            m_on_closed();
    }

    ::asio::io_context &m_io;
    ::asio::ip::tcp::socket m_socket;
    wire::stream_inbound<asio_timer, ::asio::io_context &> m_inbound;
    std::vector<std::byte> m_frame_scratch;
    std::array<std::byte, 4096> m_read_buf{};
    io::congestion m_congestion;
    std::size_t m_dropped{0};                             // congestion=drop shed count
    io::detail::stream_send_queue m_egress;               // bounded byte-budgeted serial write block
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    bool m_open{false};
};

}

#endif
