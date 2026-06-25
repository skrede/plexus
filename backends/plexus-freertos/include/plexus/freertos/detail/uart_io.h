#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_UART_IO_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_UART_IO_H

// The asio->UART transport leg, factored out of uart_channel so the channel header
// stays the byte_channel shell and stays under the file-size ceiling. These are the
// only functions that name the ESP-IDF UART driver; on-target they bind the real
// `driver/uart.h` symbols, on the host suite they bind the test-local UART shim that
// declares the same surface (see the seam test). Mirrors how serial_channel keeps its
// read loop in detail/serial_channel_io.h.

#if defined(ESP_PLATFORM)
    #include "driver/uart.h"
#endif

#include <span>
#include <cstddef>

namespace plexus::freertos::detail {

// One non-blocking RX step: drain whatever the driver's ISR-fed ring currently holds
// into the caller's fixed scratch (ticks=0 never parks the cooperative loop). Returns
// the byte count (>=0), or 0 on the driver's -1 error sentinel so the caller's feed is
// never handed a negative span length.
[[nodiscard]] inline std::size_t uart_poll_into(uart_port_t port, std::span<std::byte> scratch) noexcept
{
    const int n = uart_read_bytes(port, scratch.data(), static_cast<std::uint32_t>(scratch.size()), 0);
    return n > 0 ? static_cast<std::size_t>(n) : 0u;
}

// Synchronous egress: with tx_buffer_size==0 each call blocks only until the bytes are
// in the hardware FIFO — deterministic for the low-rate one-message path. The header
// and the CRC trailer are written as two adjacent calls (the host serial path's two
// gather nodes collapsed to two synchronous writes).
inline void uart_write_all(uart_port_t port, std::span<const std::byte> bytes) noexcept
{
    uart_write_bytes(port, bytes.data(), bytes.size());
}

// The overrun probe. With Option A (no event queue) there is no UART_FIFO_OVF /
// UART_BUFFER_FULL event to dequeue, so a near-full ISR ring is the observable proxy:
// if the buffered length has reached the ring ceiling a byte is about to be (or has
// been) dropped by the driver. The caller surfaces this; it is never swallowed. The
// on-target reality of this signal is proven in the cross-build + live gate.
[[nodiscard]] inline bool uart_ring_overrun(uart_port_t port, std::size_t ring_ceiling) noexcept
{
    std::size_t buffered = 0;
    uart_get_buffered_data_len(port, &buffered);
    return buffered >= ring_ceiling;
}

}

#endif
