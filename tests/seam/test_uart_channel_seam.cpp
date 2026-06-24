// Host seam proof for the constrained-target UART byte_channel: the static_assert
// fires at compile time (the load-bearing proof that uart_channel satisfies the
// byte_channel concept), and the runtime TEST_CASEs drive synthetic bytes through its
// reused crc_serial decorator path WITHOUT a real UART — over the test-local host UART
// shim. The on-target uart_read_bytes/uart_write_bytes path is exercised for real only
// in the cross-build + live gate; this TU is additive to the host suite baseline.

#include "host_uart_shim.h" // MUST precede uart_channel.h: declares the host UART symbols

#include "plexus/mcu/uart_channel.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <algorithm>
#include <initializer_list>

// The explicit witness at the test site (mirrors the gate in the header).
static_assert(plexus::io::byte_channel<plexus::mcu::uart_channel>, "uart_channel must satisfy byte_channel at the seam-test site");

namespace {

constexpr std::size_t k_max_payload = 512;
constexpr std::size_t k_ring        = 2048;

// A complete on-wire byte sequence (header + payload + CRC32C trailer) for the bytes a
// peer would put on the link — exactly what the decorator expects to verify and emit.
std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    auto frame = plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                            payload);
    const auto header  = std::span<const std::byte>{frame}.first(plexus::wire::header_size);
    const auto body    = std::span<const std::byte>{frame}.subspan(plexus::wire::header_size);
    const auto trailer = plexus::stream::crc_trailer(header, body);
    frame.insert(frame.end(), trailer.begin(), trailer.end());
    return frame;
}

std::vector<std::byte> bytes_of(std::initializer_list<int> vs)
{
    std::vector<std::byte> out;
    for(int v : vs)
        out.push_back(std::byte(v));
    return out;
}

}

TEST_CASE("uart_channel delivers a verified frame synchronously, no owning per-frame copy", "[seam]")
{
    plexus::test::reset_uart_fixture();

    const auto payload              = bytes_of({0xDE, 0xAD, 0xBE, 0xEF});
    plexus::test::uart_fixture().rx = on_wire(payload);

    plexus::mcu::uart_channel ch{0, k_max_payload, k_ring};

    std::vector<std::byte> received;
    bool protocol_closed = false;
    ch.on_data([&](std::span<const std::byte> frame) { received.assign(frame.begin(), frame.end()); });
    ch.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });

    ch.poll();

    // The delivered span is the COMPLETE header-on frame (header + payload), never the
    // stripped payload — the byte_channel on_data contract.
    REQUIRE(received.size() == plexus::wire::header_size + payload.size());
    const auto inner = std::span<const std::byte>{received}.subspan(plexus::wire::header_size);
    REQUIRE(std::vector<std::byte>(inner.begin(), inner.end()) == payload);
    REQUIRE_FALSE(protocol_closed);
}

TEST_CASE("uart_channel drops+resyncs a garbled stream and never wedges", "[seam]")
{
    plexus::test::reset_uart_fixture();

    // Leading line noise + a magic anchor with a corrupt trailer, then a clean frame.
    auto garbage       = bytes_of({0x00, 0xFF, 0x13, 0x37});
    const auto payload = bytes_of({0x01, 0x02, 0x03});
    auto bad           = on_wire(payload);
    bad.back()         = std::byte(bad.back() == std::byte{0} ? 0x01 : 0x00); // flip the CRC
    auto good          = on_wire(payload);

    auto &rx = plexus::test::uart_fixture().rx;
    rx.insert(rx.end(), garbage.begin(), garbage.end());
    rx.insert(rx.end(), bad.begin(), bad.end());
    rx.insert(rx.end(), good.begin(), good.end());

    plexus::mcu::uart_channel ch{0, k_max_payload, k_ring};

    int deliveries       = 0;
    bool protocol_closed = false;
    ch.on_data([&](std::span<const std::byte>) { ++deliveries; });
    ch.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });

    REQUIRE_NOTHROW(ch.poll()); // a hostile/garbled stream must never abort the channel

    REQUIRE(ch.dropped_count() >= 1); // the corrupt frame was dropped (non-fatal, counted)
    REQUIRE_FALSE(protocol_closed);   // a CRC drop NEVER reaches the fatal close seam
    REQUIRE(deliveries == 1);         // the following clean frame still resynced + delivered
}

TEST_CASE("uart_channel egress appends the reused CRC trailer", "[seam]")
{
    plexus::test::reset_uart_fixture();

    plexus::mcu::uart_channel ch{0, k_max_payload, k_ring};

    const auto payload = bytes_of({0x11, 0x22});
    const auto frame   = plexus::wire::encode_frame(
            plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, payload);
    ch.send(frame);

    // The header+payload then the 4-byte CRC32C trailer landed on the wire.
    const auto &tx = plexus::test::uart_fixture().tx;
    REQUIRE(tx.size() == frame.size() + plexus::stream::crc_trailer_size);
    REQUIRE(std::equal(frame.begin(), frame.end(), tx.begin()));

    const auto header  = std::span<const std::byte>{frame}.first(plexus::wire::header_size);
    const auto body    = std::span<const std::byte>{frame}.subspan(plexus::wire::header_size);
    const auto trailer = plexus::stream::crc_trailer(header, body);
    REQUIRE(std::equal(trailer.begin(), trailer.end(), tx.end() - plexus::stream::crc_trailer_size));
}

TEST_CASE("uart_channel surfaces a driver-ring overrun, never swallows it", "[seam]")
{
    plexus::test::reset_uart_fixture();
    plexus::test::uart_fixture().buffered = k_ring; // ring at the ceiling => overrun proxy

    plexus::mcu::uart_channel ch{0, k_max_payload, k_ring};

    int errors = 0;
    ch.on_error([&](plexus::io::io_error) { ++errors; });

    ch.poll();

    REQUIRE(ch.overrun_count() == 1); // observable counter incremented
    REQUIRE(errors == 1);             // and the on_error seam fired
}
