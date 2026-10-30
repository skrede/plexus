#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_UART_IO_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_UART_IO_H

// On target these bind the ESP-IDF driver symbols; the host suite supplies a shim declaring the same surface.
#if defined(ESP_PLATFORM)
    #include "driver/uart.h"
#endif

#include <span>
#include <cstddef>
#include <cstdint>

namespace plexus::freertos::detail {

// Zero ticks, so a read never parks the cooperative loop.
inline std::size_t uart_poll_into(uart_port_t port, std::span<std::byte> scratch) noexcept
{
    const int n = uart_read_bytes(port, scratch.data(), static_cast<std::uint32_t>(scratch.size()), 0);
    return n > 0 ? static_cast<std::size_t>(n) : 0u;
}

// With tx_buffer_size == 0 each call blocks only until the bytes are in the hardware FIFO.
inline void uart_write_all(uart_port_t port, std::span<const std::byte> bytes) noexcept
{
    uart_write_bytes(port, bytes.data(), bytes.size());
}

// Without a driver event queue there is no FIFO-overflow event to dequeue, so a ring buffered up to its ceiling
// stands in for a dropped byte.
inline bool uart_ring_overrun(uart_port_t port, std::size_t ring_ceiling) noexcept
{
    std::size_t buffered = 0;
    uart_get_buffered_data_len(port, &buffered);
    return buffered >= ring_ceiling;
}

}

#endif
