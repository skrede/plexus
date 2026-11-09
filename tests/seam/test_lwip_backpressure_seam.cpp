// Must precede lwip_channel.h, which needs the host POSIX stream_socket it declares.
#include "host_tcp_socket.h"
#include "host_loopback_pair.h"

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::test::host_tcp_socket;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

namespace {

std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                      payload);
}

std::vector<std::byte> filler(std::size_t n)
{
    return std::vector<std::byte>(n, std::byte{0x5A});
}

lwip_channel make_egress_channel(host_tcp_socket socket, plexus::freertos::freertos_executor &ex, std::size_t egress_cap, plexus::io::congestion c)
{
    using lim = plexus::freertos::lwip_channel_limits;
    return lwip_channel{std::move(socket), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}, lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, egress_cap, c};
}

lwip_channel make_faultable_sender(host_tcp_socket socket, plexus::freertos::freertos_executor &ex, const bool &stall, const bool &fault)
{
    socket.use_stall_flag(stall);
    socket.use_fault_flag(fault);
    return make_egress_channel(std::move(socket), ex, plexus::freertos::lwip_channel_limits::egress_cap_bytes, plexus::io::congestion::block);
}
}

TEST_CASE("lwip_channel under block backpressures an over-cap send via would_block, never re-dials", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool stall = true;
    // Keeps the first frame resident so the second overflows the cap.
    pair->dialed.use_stall_flag(stall);

    plexus::freertos::freertos_executor ex;
    const auto frame = on_wire(filler(64));
    auto sender      = make_egress_channel(std::move(pair->dialed), ex, frame.size() + 8, plexus::io::congestion::block);

    std::int32_t errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });

    sender.send(frame);
    sender.send(frame);

    REQUIRE(errors == 1);
    REQUIRE_FALSE(sender.closed());
    REQUIRE(sender.dropped() == 0);
}

TEST_CASE("lwip_channel under drop_newest sheds + counts an over-cap send, never fires on_error", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool stall = true;
    pair->dialed.use_stall_flag(stall);

    plexus::freertos::freertos_executor ex;
    const auto frame = on_wire(filler(64));
    auto sender      = make_egress_channel(std::move(pair->dialed), ex, frame.size() + 8, plexus::io::congestion::drop_newest);

    std::int32_t errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });

    sender.send(frame);
    sender.send(frame);

    REQUIRE(sender.dropped() == 1);
    REQUIRE(errors == 0);
    REQUIRE_FALSE(sender.closed());
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

    std::int32_t errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });
    std::vector<std::byte> received;
    std::int32_t deliveries = 0;
    receiver.on_data(
            [&](std::span<const std::byte> f)
            {
                received.assign(f.begin(), f.end());
                ++deliveries;
            });

    const auto payload = filler(32);
    sender.send(on_wire(payload));

    for(std::int32_t turn = 0; turn < 50; ++turn)
    {
        sender.poll();
        receiver.poll();
    }
    REQUIRE(deliveries == 0);
    REQUIRE(errors == 0);
    REQUIRE_FALSE(sender.closed());

    stall = false;
    plexus::test::poll_until(
            [&]
            {
                sender.poll();
                receiver.poll();
            },
            [&] { return !(deliveries == 0); });

    REQUIRE(deliveries == 1);
    REQUIRE(received.size() == plexus::wire::header_size + payload.size());
}

TEST_CASE("lwip_channel surfaces a hard send drop as on_error, distinct from protocol close", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool fault = true;
    // The next send fails with the hard ECONNRESET/EPIPE class.
    pair->dialed.use_fault_flag(fault);

    plexus::freertos::freertos_executor ex;
    auto sender = make_egress_channel(std::move(pair->dialed), ex, plexus::freertos::lwip_channel_limits::egress_cap_bytes, plexus::io::congestion::block);

    std::int32_t errors = 0;
    bool protocol_close = false;
    sender.on_error([&](plexus::io::io_error) { ++errors; });
    sender.on_protocol_close([&](plexus::wire::close_cause) { protocol_close = true; });

    sender.send(on_wire(filler(64)));

    REQUIRE(errors == 1);
    REQUIRE_FALSE(protocol_close);
    REQUIRE(sender.closed());
}

TEST_CASE("lwip_channel ends a poll turn whose stalled-send retry hit a hard drop without receiving on the closed socket", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    bool stall = true;
    bool fault = false;
    plexus::freertos::freertos_executor ex;
    auto sender         = make_faultable_sender(std::move(pair->dialed), ex, stall, fault);
    std::int32_t errors = 0;
    sender.on_error([&](plexus::io::io_error) { ++errors; });

    sender.send(on_wire(filler(32)));
    sender.poll();
    REQUIRE(errors == 0);

    stall                      = false;
    fault                      = true;
    const std::uint32_t before = plexus::test::host_recv_calls.load();
    sender.poll();

    CHECK(errors == 1);
    CHECK(sender.closed());
    CHECK(plexus::test::host_recv_calls.load() == before);
}
