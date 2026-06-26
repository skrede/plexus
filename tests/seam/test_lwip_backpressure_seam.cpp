// Host slice for the constrained-target lwIP egress backpressure / error-class split: over a REAL
// localhost-TCP loopback pair (a genuine POSIX stream_socket, no hardware and no mock) it proves the
// two failure classes stay DISTINCT. A full egress queue (cap exceeded) drives the congestion QoS —
// block backpressures the producer (would_block), drop_newest sheds + counts — and NEVER fires
// on_error / re-dials (the channel stays open). A transient short/zero send re-arms with the unsent
// tail left queued (no success-of-zero-bytes), flushing once the stall clears. A hard ECONNRESET/EPIPE
// fires on_error(connection_reset), DISTINCT from on_protocol_close. Additive to the host suite.

#include "host_tcp_socket.h" // MUST precede lwip_channel.h: declares the host POSIX stream_socket

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/congestion.h"
#include "plexus/io/io_error.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>

using plexus::test::host_tcp_socket;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

namespace {

std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, payload);
}

std::vector<std::byte> filler(std::size_t n)
{
    return std::vector<std::byte>(n, std::byte{0x5A});
}

// A channel with a chosen egress cap + congestion policy; the read/message/reassembly knobs keep the
// channel defaults (this slice exercises only the egress half).
lwip_channel make_egress_channel(host_tcp_socket socket, plexus::freertos::freertos_executor &ex, std::size_t egress_cap, plexus::io::congestion c)
{
    using lim = plexus::freertos::lwip_channel_limits;
    return lwip_channel{std::move(socket), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}, lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, egress_cap, c};
}

}

TEST_CASE("lwip_channel under block backpressures an over-cap send via would_block, never re-dials", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool stall = true;
    pair->dialed.use_stall_flag(stall); // keep the first frame resident so the second overflows the cap

    plexus::freertos::freertos_executor ex;
    const auto frame  = on_wire(filler(64));
    auto       sender = make_egress_channel(std::move(pair->dialed), ex, frame.size() + 8, plexus::io::congestion::block);

    int errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });

    sender.send(frame); // admitted, stalls resident (the cap now near-full)
    sender.send(frame); // would exceed the cap -> congestion=block -> would_block

    REQUIRE(errors == 1);            // backpressure surfaced through on_error(would_block)
    REQUIRE_FALSE(sender.closed());  // congestion never tears the channel down
    REQUIRE(sender.dropped() == 0);  // block does not shed
}

TEST_CASE("lwip_channel under drop_newest sheds + counts an over-cap send, never fires on_error", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool stall = true;
    pair->dialed.use_stall_flag(stall);

    plexus::freertos::freertos_executor ex;
    const auto frame  = on_wire(filler(64));
    auto       sender = make_egress_channel(std::move(pair->dialed), ex, frame.size() + 8, plexus::io::congestion::drop_newest);

    int errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });

    sender.send(frame); // admitted, stalls resident
    sender.send(frame); // over cap -> drop_newest -> shed + count

    REQUIRE(sender.dropped() == 1);  // the overrun frame was shed and counted
    REQUIRE(errors == 0);            // drop_newest is silent, never on_error
    REQUIRE_FALSE(sender.closed());  // the channel stays open
}

TEST_CASE("lwip_channel re-arms a transient short send and flushes once cleared, no zero-byte success", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool stall = true;
    pair->dialed.use_stall_flag(stall);

    plexus::freertos::freertos_executor ex;
    auto sender   = make_egress_channel(std::move(pair->dialed), ex, plexus::freertos::lwip_channel_limits::egress_cap_bytes, plexus::io::congestion::block);
    auto receiver = lwip_channel{std::move(pair->accepted), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}};

    int errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });
    std::vector<std::byte> received;
    int                    deliveries = 0;
    receiver.on_data([&](std::span<const std::byte> f)
                     {
                         received.assign(f.begin(), f.end());
                         ++deliveries;
                     });

    const auto payload = filler(32);
    sender.send(on_wire(payload)); // stalled: the gather stays resident, nothing leaves the node

    for(int turn = 0; turn < 50; ++turn)
    {
        sender.poll();   // pumps the stalled egress — still stalled, must not pop or error
        receiver.poll();
    }
    REQUIRE(deliveries == 0); // a stalled send delivers nothing — no success-of-zero-bytes
    REQUIRE(errors == 0);     // a soft stall is not an error
    REQUIRE_FALSE(sender.closed());

    stall = false; // the socket can flush now
    for(int turn = 0; turn < 100 && deliveries == 0; ++turn)
    {
        sender.poll();
        receiver.poll();
    }

    REQUIRE(deliveries == 1); // the unsent tail flushed exactly once after the re-arm
    REQUIRE(received.size() == plexus::wire::header_size + payload.size());
}

TEST_CASE("lwip_channel surfaces a hard send drop as on_error, distinct from protocol close", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool fault = true;
    pair->dialed.use_fault_flag(fault); // the next send draws the hard ECONNRESET/EPIPE class

    plexus::freertos::freertos_executor ex;
    auto sender = make_egress_channel(std::move(pair->dialed), ex, plexus::freertos::lwip_channel_limits::egress_cap_bytes, plexus::io::congestion::block);

    int  errors         = 0;
    bool protocol_close = false;
    sender.on_error([&](plexus::io::io_error) { ++errors; });
    sender.on_protocol_close([&](plexus::wire::close_cause) { protocol_close = true; });

    sender.send(on_wire(filler(64))); // a hard send failure must reach on_error, never congestion

    REQUIRE(errors == 1);          // the hard-drop seam fired on the send side
    REQUIRE_FALSE(protocol_close); // DISTINCT from the framing-violation seam
    REQUIRE(sender.closed());      // a hard drop closes the socket
}
