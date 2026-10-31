// The shim declares the host UART symbols uart_channel.h names, so it comes first.
#include "host_uart_shim.h"

#include "support/serial_bytes.h"

#include "plexus/freertos/uart_channel.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/stream/serial_frame.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/io/io_error.h"
#include "plexus/io/detail/drop_event.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace {

constexpr std::size_t k_max_payload = 512;
constexpr std::size_t k_ring        = 2048;
constexpr std::size_t k_over_cap    = 600;
constexpr std::size_t k_fitting     = 16;

std::vector<std::byte> frame_of(std::size_t payload_len)
{
    const std::vector<std::byte> payload(payload_len, std::byte{0x11});
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                      payload);
}

std::vector<std::byte> on_wire(std::size_t payload_len)
{
    return plexus::testing::serial_bytes(frame_of(payload_len));
}

}

TEST_CASE("uart_channel cap: a frame over the leaf's cap is reported as payload_too_large on its drop seam", "[seam]")
{
    plexus::test::reset_uart_fixture();
    const auto over_cap = on_wire(k_over_cap);
    const auto fitting  = on_wire(k_fitting);
    auto &rx            = plexus::test::uart_fixture().rx;
    rx.insert(rx.end(), over_cap.begin(), over_cap.end());
    rx.insert(rx.end(), fitting.begin(), fitting.end());

    plexus::freertos::freertos_executor ex;
    plexus::freertos::uart_channel ch{0, ex, k_max_payload, k_ring};

    std::vector<plexus::wire::close_cause> causes;
    std::vector<std::size_t> delivered;
    ch.on_data([&](std::span<const std::byte> frame) { delivered.push_back(frame.size()); });
    ch.on_frame_dropped([&](plexus::wire::close_cause cause) { causes.push_back(cause); });

    ch.poll();

    REQUIRE(causes == std::vector<plexus::wire::close_cause>{plexus::wire::close_cause::payload_too_large});
    REQUIRE(ch.dropped_count() == 1);
    REQUIRE(delivered == std::vector<std::size_t>{plexus::wire::header_size + k_fitting});
}

TEST_CASE("uart_channel cap: the leaf reports the serial ceiling of its cap", "[seam]")
{
    plexus::test::reset_uart_fixture();
    plexus::freertos::freertos_executor ex;
    plexus::freertos::uart_channel ch{0, ex, k_max_payload, k_ring};

    REQUIRE(ch.max_frame_bytes() == plexus::stream::serial_frame_ceiling(k_max_payload));
}

TEST_CASE("uart_channel cap: a frame over the leaf's ceiling is refused and the leaf keeps sending", "[seam]")
{
    plexus::test::reset_uart_fixture();
    plexus::freertos::freertos_executor ex;
    plexus::freertos::uart_channel ch{0, ex, k_max_payload, k_ring};

    std::vector<plexus::io::io_error> errors;
    std::int32_t oversize_drops = 0;
    ch.on_error([&](plexus::io::io_error e) { errors.push_back(e); });
    ch.on_drop([&](const plexus::io::detail::drop_event &e) { oversize_drops += (e.cause == plexus::io::detail::drop_cause::oversize); });
    const auto &tx = plexus::test::uart_fixture().tx;

    ch.send(frame_of(k_max_payload));
    REQUIRE(tx == on_wire(k_max_payload));

    ch.send(frame_of(k_max_payload + 1));
    REQUIRE(tx == on_wire(k_max_payload));
    REQUIRE(errors == std::vector<plexus::io::io_error>{plexus::io::io_error::message_too_large});
    REQUIRE(oversize_drops == 1);

    ch.send(frame_of(k_fitting));
    auto expected      = on_wire(k_max_payload);
    const auto fitting = on_wire(k_fitting);
    expected.insert(expected.end(), fitting.begin(), fitting.end());
    REQUIRE(tx == expected);
}
