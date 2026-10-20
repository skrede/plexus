#include "host_uart_shim.h"
#include "host_tcp_socket.h"
#include "host_loopback_pair.h"

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/uart_channel.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"

#include "plexus/stream/crc_serial.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

using plexus::test::host_tcp_socket;
using plexus::freertos::uart_channel;
using plexus::freertos::freertos_executor;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

namespace {

constexpr std::array<std::byte, 4> k_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
constexpr std::size_t k_max_payload = 512;
constexpr std::size_t k_ring        = 2048;

std::vector<std::byte> framed()
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                      std::span<const std::byte>{k_payload});
}

std::vector<std::byte> framed_with_crc()
{
    auto frame         = framed();
    const auto header  = std::span<const std::byte>{frame}.first(plexus::wire::header_size);
    const auto body    = std::span<const std::byte>{frame}.subspan(plexus::wire::header_size);
    const auto trailer = plexus::stream::crc_trailer(header, body);
    frame.insert(frame.end(), trailer.begin(), trailer.end());
    return frame;
}

struct lwip_pair
{
    lwip_pair()
            : ex()
            , pair(plexus::test::make_loopback_pair())
            , sender(std::move(pair.value().dialed))
            , channel(std::make_unique<lwip_channel>(std::move(pair.value().accepted), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}))
    {
    }

    freertos_executor ex;
    std::optional<plexus::test::loopback_pair> pair;
    host_tcp_socket sender;
    std::unique_ptr<lwip_channel> channel;
};

bool rx_turn(lwip_channel &channel, freertos_executor &ex)
{
    auto *slot = channel.acquire_rx_slot(0);
    if(!slot)
        return false;
    const std::size_t n = channel.recv_into_slot(*slot);
    if(n > 0)
        ex.post_from_task(plexus::freertos::posted_work{&lwip_channel::invoke_feed, slot});
    else
        channel.release_rx_slot(*slot);
    return n > 0;
}

}

TEST_CASE("freertos_retire_quiescence closing an lwIP channel posts its on_closed through the executor once", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    lwip_pair p;
    std::int32_t closes = 0;
    p.channel->on_closed([&closes] { ++closes; });

    p.channel->close();
    p.channel->close();
    CHECK(closes == 0);
    p.ex.drain();
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence an lwIP handler installed after the close is posted", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    lwip_pair p;
    p.channel->close();
    p.ex.drain();

    std::int32_t closes = 0;
    p.channel->on_closed([&closes] { ++closes; });
    CHECK(closes == 0);
    p.ex.drain();
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence an lwIP delivery received before the close is dropped after it", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    lwip_pair p;
    std::int32_t deliveries = 0;
    p.channel->on_data([&deliveries](std::span<const std::byte>) { ++deliveries; });
    p.sender.send(framed());

    bool got = false;
    plexus::test::poll_until([&] { got = got || rx_turn(*p.channel, p.ex); }, [&] { return got; });
    REQUIRE(got);
    p.channel->close();
    p.ex.drain();
    CHECK(deliveries == 0);
}

TEST_CASE("freertos_retire_quiescence a peer's close fails the lwIP channel once and posts its on_closed", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    lwip_pair p;
    std::int32_t errors = 0;
    std::int32_t closes = 0;
    p.channel->on_error([&errors](plexus::io::io_error) { ++errors; });
    p.channel->on_closed([&closes] { ++closes; });

    p.sender.close();
    plexus::test::poll_until([&] { p.channel->poll(); }, [&] { return errors != 0; });
    for(std::int32_t turn = 0; turn < 4; ++turn)
        p.channel->poll();
    p.ex.drain();
    CHECK(errors == 1);
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence closing a UART channel posts its on_closed through the executor once", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    plexus::test::reset_uart_fixture();
    freertos_executor ex;
    uart_channel ch{0, k_max_payload, k_ring};
    std::int32_t closes = 0;
    ch.on_closed([&closes] { ++closes; });

    ch.close();
    ch.close();
    CHECK(closes == 0);
    ex.drain();
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence a UART handler installed after the close is posted", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    plexus::test::reset_uart_fixture();
    freertos_executor ex;
    uart_channel ch{0, k_max_payload, k_ring};
    ch.close();
    ex.drain();

    std::int32_t closes = 0;
    ch.on_closed([&closes] { ++closes; });
    CHECK(closes == 0);
    ex.drain();
    CHECK(closes == 1);
}

TEST_CASE("freertos_retire_quiescence a UART frame polled after the close is dropped", "[seam][freertos_retire_quiescence][!shouldfail]")
{
    plexus::test::reset_uart_fixture();
    plexus::test::uart_fixture().rx = framed_with_crc();
    freertos_executor ex;
    uart_channel ch{0, k_max_payload, k_ring};
    std::int32_t deliveries = 0;
    ch.on_data([&deliveries](std::span<const std::byte>) { ++deliveries; });

    ch.close();
    ch.poll();
    ex.drain();
    CHECK(deliveries == 0);
}
