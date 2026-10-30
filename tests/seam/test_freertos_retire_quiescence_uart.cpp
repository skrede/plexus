#include "host_uart_shim.h"

#include "support/serial_bytes.h"

#include "plexus/freertos/uart_channel.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::freertos::uart_channel;
using plexus::freertos::freertos_executor;

namespace {

constexpr std::array<std::byte, 4> k_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
constexpr std::size_t k_max_payload = 512;
constexpr std::size_t k_ring        = 2048;

std::vector<std::byte> framed_with_crc()
{
    return plexus::testing::serial_bytes(
            plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                       std::span<const std::byte>{k_payload}));
}

}

TEST_CASE("freertos_retire_quiescence closing a UART channel posts its on_closed through the executor once", "[seam][freertos_retire_quiescence]")
{
    plexus::test::reset_uart_fixture();
    freertos_executor ex;
    uart_channel ch{0, ex, k_max_payload, k_ring};
    std::int32_t closes = 0;
    ch.on_closed([&closes] { ++closes; });

    ch.close();
    ch.close();
    CHECK(closes == 0);
    ex.drain();
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence a UART handler installed after the close is posted", "[seam][freertos_retire_quiescence]")
{
    plexus::test::reset_uart_fixture();
    freertos_executor ex;
    uart_channel ch{0, ex, k_max_payload, k_ring};
    ch.close();
    ex.drain();

    std::int32_t closes = 0;
    ch.on_closed([&closes] { ++closes; });
    CHECK(closes == 0);
    ex.drain();
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence a UART frame polled after the close is dropped", "[seam][freertos_retire_quiescence]")
{
    plexus::test::reset_uart_fixture();
    plexus::test::uart_fixture().rx = framed_with_crc();
    freertos_executor ex;
    uart_channel ch{0, ex, k_max_payload, k_ring};
    std::int32_t deliveries = 0;
    ch.on_data([&deliveries](std::span<const std::byte>) { ++deliveries; });

    ch.close();
    ch.poll();
    ex.drain();
    CHECK(deliveries == 0);
}
