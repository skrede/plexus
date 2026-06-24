#ifndef HPP_GUARD_PLEXUS_MCU_UART_CHANNEL_H
#define HPP_GUARD_PLEXUS_MCU_UART_CHANNEL_H

#include "plexus/mcu/detail/uart_io.h"

#include "plexus/io/byte_channel.h"
#include "plexus/stream/crc_serial.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <utility>
#include <cstddef>

namespace plexus::mcu {

// The constrained-target UART byte_channel: the one new transport leaf of the serial
// path. It collapses the host serial_channel + serial_channel_io + serial_bootstrap
// into a single lean header and drives the policy-free wire layer — crc_serial_inbound
// + stream_inbound + frame_codec, reused VERBATIM — over uart_read_bytes/uart_write_bytes
// instead of asio. The engine and the framing/CRC bytes are unchanged; only the asio->UART
// swap is new (the byte-identical claim lives at the wire/CRC byte level, not this class).
//
// Fully-qualify plexus::detail::move_only_function below: an io/host-shim detail namespace
// is in scope inside plexus::mcu and would shadow the bare detail:: lookup (the documented
// move_only_function shadowing pitfall, see freertos_policy.h:31-34).
class uart_channel
{
public:
    // ~2 KiB fixed RX scratch: accept the read-buffer floor (negligible on the target's
    // SRAM); the egress + per-message ceilings carry the real savings. NOT grown per read.
    static constexpr std::size_t k_scratch_bytes = 2048;

    explicit uart_channel(uart_port_t port, std::size_t max_payload, std::size_t rx_ring_ceiling)
            : m_port(port)
            , m_rx_ring_ceiling(rx_ring_ceiling)
            , m_decorator(max_payload)
    {
        wire_decorator();
    }

    void send(std::span<const std::byte> framed)
    {
        const auto header  = framed.first(plexus::wire::header_size);
        const auto payload = framed.subspan(plexus::wire::header_size);
        const auto trailer = plexus::stream::crc_trailer(header, payload); // REUSED verbatim
        detail::uart_write_all(m_port, framed);
        detail::uart_write_all(m_port, std::span<const std::byte>{trailer});
    }

    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {"serial", ""};
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

    // One cooperative super-loop step: drain the driver's ISR-fed ring into the fixed
    // scratch and feed the reused decorator. The driver overrun is surfaced (never
    // swallowed) so a lost byte is observable and just drops+resyncs one frame.
    void poll()
    {
        check_overrun();
        const std::size_t n = detail::uart_poll_into(m_port, m_scratch);
        if(n > 0)
            m_decorator.feed(std::span<const std::byte>{m_scratch.data(), n});
    }

    // The observable overrun seam: a monotonic count of detected driver-ring overruns,
    // in addition to the one-shot on_error fire — a lost byte is visible, never swallowed.
    [[nodiscard]] std::size_t overrun_count() const noexcept
    {
        return m_overrun_count;
    }

    // The observable dropped-frame seam: a monotonic count of CRC-mismatched frames the
    // decorator discarded and resynced past. This is the host on_frame_dropped seam in
    // count form — non-fatal by contract, so a corrupted frame is visible, never fatal.
    [[nodiscard]] std::size_t dropped_count() const noexcept
    {
        return m_dropped_count;
    }

private:
    void wire_decorator()
    {
        m_decorator.on_match([this](std::span<const std::byte> f) { deliver(f); });
        // A CRC mismatch is the ONE non-fatal decorator cause (crc_serial.h): the frame is
        // dropped and the link resynced, NEVER torn down. Count it (observable via
        // dropped_count) and leave the session up — mirroring the host on_frame_dropped seam.
        // It must NOT reach on_protocol_close, which stays strictly fatal.
        m_decorator.on_drop([this](plexus::wire::close_cause) { ++m_dropped_count; });
    }

    // CRITICAL DIVERGENCE from the host serial_channel::post_frame, which allocates a
    // shared owning copy of every verified frame (a heap allocation) + asio::post. That
    // per-frame throwing heap alloc is BANNED on the -fno-exceptions floor, so this
    // delivers SYNCHRONOUSLY: the frame span is consumed before poll() reuses m_scratch.
    //
    // The byte_channel concept states on_data is "ALWAYS posted onto the executor, never
    // invoked synchronously from inside feed()" (byte_channel.h:27-30), and peer_session's
    // re-entrancy safety is structural "because on_data is always posted" (peer_session.h:68).
    // This channel deliberately diverges, and it is sound here: under Option A poll() runs on
    // the SAME task that drains the executor (the super-loop does `channel.poll(); while
    // (ex.pump()){}`), so on_receive runs on the executor task and consumes the span fully
    // (decode_header + router.route) before poll() returns — the re-entrancy invariant holds
    // and the span never escapes the fixed scratch. Do NOT "fix" this back into a posted
    // delivery: that re-introduces the banned per-frame owning heap allocation.
    void deliver(std::span<const std::byte> frame)
    {
        if(m_on_data)
            m_on_data(frame);
    }

    void check_overrun()
    {
        if(!detail::uart_ring_overrun(m_port, m_rx_ring_ceiling))
            return;
        ++m_overrun_count;
        if(m_on_error)
            m_on_error(plexus::io::io_error::would_block);
    }

    uart_port_t                                                          m_port;
    std::size_t                                                          m_rx_ring_ceiling;
    std::size_t                                                          m_overrun_count{0};
    std::size_t                                                          m_dropped_count{0};
    std::array<std::byte, k_scratch_bytes>                               m_scratch{};
    plexus::stream::crc_serial_inbound                                   m_decorator;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>       m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)>  m_on_protocol_close;
};

static_assert(plexus::io::byte_channel<plexus::mcu::uart_channel>, "uart_channel must satisfy byte_channel — check the seven verbs");

}

#endif
