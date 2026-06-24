#include "test_serial_common.h"

#include "plexus/io/frame_router.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/wire/data_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <span>
#include <array>
#include <vector>
#include <string>
#include <cstddef>
#include <optional>

using namespace serial_fixture;

// Corruption injected onto an untrusted serial line must be DROPPED and the link
// magic-resynced — never aborted or closed. The corrupt bytes are written RAW onto the pty
// master fd (bypassing the egress CRC trailer, exactly as a line glitch / hostile peer would
// put them on the wire); only the slave end is adopted into a serial_channel whose decorator
// must catch the bad CRC, fire on_frame_dropped(crc_mismatch), resync on the next 0x56 0x50,
// and still deliver the following valid frame — with on_protocol_close NEVER fired. Each leg
// loops in-body; the ctest invocation is re-run >=3 process runs.

namespace {

// A complete serial wire frame: the production header-on bytes plus the decorator's 4-byte
// LE CRC32C trailer — exactly what serial egress puts on the line.
std::vector<std::byte> wire_frame(const std::string &payload)
{
    const auto framed  = make_data_frame(payload, /*session_id=*/1);
    const auto header  = std::span<const std::byte>{framed}.first(wire::header_size);
    const auto inner   = std::span<const std::byte>{framed}.subspan(wire::header_size);
    const auto trailer = stream::crc_trailer(header, inner);
    std::vector<std::byte> out{framed.begin(), framed.end()};
    out.insert(out.end(), trailer.begin(), trailer.end());
    return out;
}

void write_raw(int fd, std::span<const std::byte> bytes)
{
    REQUIRE(::write(fd, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size()));
}

struct rx_probe
{
    std::optional<std::string> received;
    int drops{0};
    bool protocol_closed{false};
    bool closed{false};
    plexus::log::null_logger sink;
    pio::frame_router router{sink};

    void wire(pasio::serial_channel &ch)
    {
        router.on_unidirectional(
                [this](const wire::frame_header &, std::span<const std::byte> inner)
                {
                    auto decoded = wire::decode_unidirectional(inner);
                    if(decoded)
                        received = to_string(decoded->data);
                });
        ch.on_data([this](std::span<const std::byte> f) { router.route(f); });
        ch.on_frame_dropped(
                [this](wire::close_cause c)
                {
                    if(c == wire::close_cause::crc_mismatch)
                        ++drops;
                });
        ch.on_protocol_close([this](wire::close_cause) { protocol_closed = true; });
        ch.on_closed([this] { closed = true; });
    }
};

}

TEST_CASE("serial resync: a byte-FLIPPED frame is dropped and the link recovers on the next frame, "
          "looped",
          "[integration][serial][resync]")
{
    constexpr int k_iterations = 25;
    int recovered              = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        const int master = pty.take_master();
        auto rx          = adopt_channel(io, pty.take_slave());

        rx_probe probe;
        probe.wire(*rx);

        auto bad = wire_frame("corrupt-flip");
        bad[wire::header_size + 2] ^= std::byte{0xFF}; // flip a payload byte -> CRC fails
        write_raw(master, bad);
        write_raw(master, wire_frame("recovered-after-flip"));

        pump_until(io, [&] { return probe.received.has_value(); });

        REQUIRE(probe.drops >= 1);                          // the corrupt frame was caught
        REQUIRE(probe.received.has_value());                // the next frame still decoded
        REQUIRE(*probe.received == "recovered-after-flip"); // resync recovered the boundary
        REQUIRE_FALSE(probe.protocol_closed);               // the link was NOT torn down
        REQUIRE_FALSE(probe.closed);
        ++recovered;
        ::close(master);
    }
    REQUIRE(recovered == k_iterations);
}

TEST_CASE("serial resync: a byte-DROPPED (truncated) frame desyncs but the next frame resyncs, "
          "looped",
          "[integration][serial][resync]")
{
    constexpr int k_iterations = 25;
    int recovered              = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        const int master = pty.take_master();
        auto rx          = adopt_channel(io, pty.take_slave());

        rx_probe probe;
        probe.wire(*rx);

        // Drop one byte mid-frame: the trailer no longer sits where the (now-shifted) length
        // implies, so the CRC check fails and the boundary desyncs. The decorator must resync
        // on the next magic anchor rather than wedge.
        auto full = wire_frame("corrupt-drop");
        std::vector<std::byte> truncated{full.begin(), full.end() - 1};
        write_raw(master, truncated);
        write_raw(master, wire_frame("recovered-after-drop"));

        pump_until(io, [&] { return probe.received.has_value(); });

        REQUIRE(probe.received.has_value());
        REQUIRE(*probe.received == "recovered-after-drop");
        REQUIRE_FALSE(probe.protocol_closed); // never a fatal close on a lossy line
        REQUIRE_FALSE(probe.closed);
        ++recovered;
        ::close(master);
    }
    REQUIRE(recovered == k_iterations);
}

TEST_CASE("serial resync: corruption is non-fatal — neither on_protocol_close nor on_closed fires", "[integration][serial][resync]")
{
    constexpr int k_iterations = 25;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pty_pair pty;
        ::asio::io_context io;
        const int master = pty.take_master();
        auto rx          = adopt_channel(io, pty.take_slave());

        rx_probe probe;
        probe.wire(*rx);

        // Two corrupt frames back to back, then a clean one: the link survives the run.
        auto bad1 = wire_frame("bad-one");
        bad1.back() ^= std::byte{0x5A};
        auto bad2 = wire_frame("bad-two");
        bad2[wire::header_size] ^= std::byte{0x33};
        write_raw(master, bad1);
        write_raw(master, bad2);
        write_raw(master, wire_frame("survivor"));

        pump_until(io, [&] { return probe.received.has_value(); });

        REQUIRE(probe.received.has_value());
        REQUIRE(*probe.received == "survivor");
        REQUIRE(probe.drops >= 1);
        REQUIRE_FALSE(probe.protocol_closed);
        REQUIRE_FALSE(probe.closed);
        ::close(master);
    }
}
