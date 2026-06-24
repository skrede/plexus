#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_UART_SHIM_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_UART_SHIM_H

// Host stand-in for the ESP-IDF UART driver surface that uart_channel names. NET-NEW
// scaffolding: the existing freertos_host_shim.h shims only the FreeRTOS kernel
// primitives (no UART symbols anywhere in the tree). It is patterned structurally after
// that shim's `#if !defined(ESP_PLATFORM)` host stand-in idiom, but covers a different
// API surface declared fresh here. It is test-local — NEVER shipped in lib/ (on-target
// the real symbols come from the IDF `driver` component, and uart_channel's
// detail/uart_io.h includes `driver/uart.h` under `#if defined(ESP_PLATFORM)`).
//
// The on-target reality of the overrun signal (uart_get_buffered_data_len) is proven
// only in the cross-build + live gate, not this host seam. Here it just lets the header
// compile and lets the test drive a synthetic RX stream / capture the TX bytes.
#if !defined(ESP_PLATFORM)

    #include <span>
    #include <vector>
    #include <cstddef>
    #include <cstdint>
    #include <algorithm>

using uart_port_t = int;

namespace plexus::test {

// The host UART fixture the shim free functions read/write. A test seeds rx with the
// bytes the channel should "receive", reads tx back to assert the egress, and sets
// buffered to force the overrun probe.
struct host_uart_state
{
    std::vector<std::byte> rx; // bytes the next uart_read_bytes hands over
    std::size_t rx_pos{0};     // read cursor into rx
    std::vector<std::byte> tx; // bytes uart_write_bytes captured
    std::size_t buffered{0};   // value uart_get_buffered_data_len reports
};

inline host_uart_state &uart_fixture()
{
    static host_uart_state state;
    return state;
}

inline void reset_uart_fixture()
{
    uart_fixture() = host_uart_state{};
}

}

// Hand over at most `len` not-yet-read bytes from the fixture's rx buffer (0 when
// drained) — the non-blocking ticks=0 contract: returns whatever the ring holds.
inline int uart_read_bytes(uart_port_t, void *buf, std::uint32_t len, std::uint32_t /*ticks*/)
{
    auto &fx             = plexus::test::uart_fixture();
    const auto available = fx.rx.size() - fx.rx_pos;
    const auto n         = std::min<std::size_t>(available, len);
    std::copy_n(fx.rx.data() + fx.rx_pos, n, static_cast<std::byte *>(buf));
    fx.rx_pos += n;
    return static_cast<int>(n);
}

inline int uart_write_bytes(uart_port_t, const void *src, std::size_t size)
{
    auto &fx      = plexus::test::uart_fixture();
    const auto *p = static_cast<const std::byte *>(src);
    fx.tx.insert(fx.tx.end(), p, p + size);
    return static_cast<int>(size);
}

inline int uart_get_buffered_data_len(uart_port_t, std::size_t *out)
{
    if(out)
        *out = plexus::test::uart_fixture().buffered;
    return 0;
}

#endif

#endif
