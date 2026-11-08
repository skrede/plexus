// Host-side test of the constrained-target lwIP P2 RX-task receive policy: off-target the spawned RX
// task does not run (the host xTaskCreate is a no-op stub), so this models the RX task's cross-context
// edge by driving the channel's acquire-slot -> recv_into -> post_from_task step directly as the
// "RX context", then ex.drain() as the executor task. It proves delivery is POSTED (on_data does NOT
// fire until the executor drains — the key P2-vs-P1 distinction) and exactly once, with reassembly
// across recv turns and a peer close surfaced as on_error. The pool is fixed: no per-recv allocation
// (the asan tree is the witness).

#include "lwip_rx_rig.h"

#include "support/retire_drain.h"

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/freertos_host_shim.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <limits>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

using plexus::test::host_tcp_socket;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

namespace {

std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                      payload);
}

std::vector<std::byte> bytes_of(std::initializer_list<int> vs)
{
    std::vector<std::byte> out;
    for(int v : vs)
        out.push_back(std::byte(v));
    return out;
}

lwip_channel make_channel(host_tcp_socket socket, plexus::freertos::freertos_executor &ex)
{
    return lwip_channel{std::move(socket), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}};
}

// One RX-context turn: acquire a free pool slot, recv into it, and post the feed across to the
// executor task — the steps the spawned RX task runs on-target, driven inline off-target. The queue
// never fills here, so the post takes a zero wait and must be taken.
bool rx_turn(lwip_channel &channel, plexus::freertos::freertos_executor &ex)
{
    auto *slot = channel.rx().acquire(0);
    if(!slot)
        return false;
    const std::size_t n = channel.rx().recv_into(*slot);
    if(n > 0)
        REQUIRE(ex.post_from_task(plexus::freertos::posted_work{&lwip_channel::rx_type::invoke_feed, slot}, 0));
    else
        channel.rx().release(*slot);
    return n > 0;
}

}

TEST_CASE("lwip rx task delivers a framed message POSTED, exactly once", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    plexus::freertos::freertos_executor ex;
    host_tcp_socket sender = std::move(pair->dialed);
    auto receiver          = make_channel(std::move(pair->accepted), ex);

    int deliveries = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++deliveries; });

    const auto payload = bytes_of({0xDE, 0xAD, 0xBE, 0xEF});
    sender.send(on_wire(payload));

    bool got = false;
    plexus::test::poll_until([&] { got = got || rx_turn(receiver, ex); }, [&] { return got; });
    REQUIRE(deliveries == 0); // POSTED: nothing delivered until the executor task drains

    ex.drain();
    REQUIRE(deliveries == 1); // exactly one delivery on the executor task, the cross-context pair to P1's sync
}

TEST_CASE("lwip rx task reassembles a split frame into one posted delivery", "[seam]")
{
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());

    plexus::freertos::freertos_executor ex;
    host_tcp_socket sender = std::move(pair->dialed);
    auto receiver          = make_channel(std::move(pair->accepted), ex);

    int deliveries = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++deliveries; });

    const auto frame = on_wire(bytes_of({0x01, 0x02, 0x03, 0x04, 0x05}));
    const auto head  = std::span<const std::byte>{frame}.first(plexus::wire::header_size);
    const auto tail  = std::span<const std::byte>{frame}.subspan(plexus::wire::header_size);

    sender.send(head);
    for(int turn = 0; turn < 50; ++turn)
        rx_turn(receiver, ex);
    ex.drain();
    REQUIRE(deliveries == 0); // the header alone is a partial frame — nothing delivered yet

    sender.send(tail);
    bool got = false;
    plexus::test::poll_until([&] { got = got || rx_turn(receiver, ex); }, [&] { return got; });
    ex.drain();
    REQUIRE(deliveries == 1); // the split frame reassembled into exactly one posted delivery
}

TEST_CASE("lwip rx task surfaces a peer close as on_error, never on_protocol_close", "[seam]")
{
    plexus::test::rx_rig rig;
    int errors          = 0;
    bool protocol_close = false;
    rig.channel->on_error([&](plexus::io::io_error) { ++errors; });
    rig.channel->on_protocol_close([&](plexus::wire::close_cause) { protocol_close = true; });
    REQUIRE(rig.spawn() == pdTRUE);

    rig.sender.close();
    plexus::test::run_spawned_task();
    REQUIRE(errors == 0);
    rig.ex.drain();

    REQUIRE(errors == 1);
    REQUIRE_FALSE(protocol_close);
}

TEST_CASE("lwip rx trampoline self-deletes on channel close instead of returning off the task", "[seam]")
{
    plexus::test::rx_rig rig;
    REQUIRE(rig.spawn() == pdTRUE);

    rig.sender.close();
    const int before = plexus::freertos::detail::host_vtask_delete_calls;
    plexus::test::run_spawned_task();
    REQUIRE(plexus::freertos::detail::host_vtask_delete_calls == before + 1);
    REQUIRE(plexus::freertos::detail::host_vtask_delete_last == nullptr);

    const auto outcome = plexus::testing::retire_and_drain(rig.channel, [&rig] { rig.ex.drain(); });
    CHECK(outcome.erased == 1);
    CHECK(outcome.empty);
}

TEST_CASE("lwip rx task: a receive timeout that is not positive fails closed", "[seam][lwip_rx_task]")
{
    const plexus::freertos::task_options task{4096};
    CHECK_THROWS_AS(plexus::freertos::rx_task_options(task, std::chrono::milliseconds{0}), std::runtime_error);
    CHECK_THROWS_AS(plexus::freertos::rx_task_options(task, std::chrono::milliseconds{-1}), std::runtime_error);
}

TEST_CASE("lwip rx task: a receive timeout past the 32-bit tick range fails closed and the largest in range is accepted", "[seam][lwip_rx_task]")
{
    const plexus::freertos::task_options task{4096};
    constexpr std::int64_t max = std::numeric_limits<std::uint32_t>::max() / 1000;
    CHECK_THROWS_AS(plexus::freertos::rx_task_options(task, std::chrono::milliseconds{max + 1}), std::runtime_error);
    CHECK_NOTHROW(plexus::freertos::rx_task_options(task, std::chrono::milliseconds{max}));
    CHECK_NOTHROW(plexus::freertos::rx_task_options(task, std::chrono::milliseconds{1}));
}

TEST_CASE("lwip rx task: a receive timeout one millisecond under a tick fails closed and only a target build tells it from zero", "[seam][lwip_rx_task]")
{
    const plexus::freertos::task_options task{4096};
    CHECK_THROWS_AS(plexus::freertos::rx_task_options(task, std::chrono::milliseconds{portTICK_PERIOD_MS - 1}), std::runtime_error);
}
