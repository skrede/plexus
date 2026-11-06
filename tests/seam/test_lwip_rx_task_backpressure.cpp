// The lwIP RX step on a full executor queue, with a host thread standing in for the RX task. The host shim gives each
// thread its own task handle and parks a waiting queue send on a condition variable, so these cases show the wait and
// its release across threads; they do not show FreeRTOS task scheduling.

#include "lwip_rx_rig.h"

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/freertos_executor.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

using plexus::test::on_wire;
using plexus::test::host_tcp_socket;
using plexus::test::acquirable_slots;
using plexus::freertos::freertos_executor;
using lwip_channel = plexus::freertos::lwip_channel<host_tcp_socket>;

namespace {

void count_up(void *ctx) noexcept
{
    ++*static_cast<std::uint32_t *>(ctx);
}

host_tcp_socket blocking(host_tcp_socket socket)
{
    socket.set_blocking(true);
    return socket;
}

void fill_queue(freertos_executor &ex, std::uint32_t &filled)
{
    for(std::uint32_t i = 0; i < freertos_executor::k_queue_depth; ++i)
        REQUIRE(ex.post_from_task(plexus::freertos::posted_work{&count_up, &filled}, 0));
}

bool rx_task_parked()
{
    return plexus::test::poll_until([] {}, [] { return plexus::freertos::detail::host_parked_senders.load() == 1; });
}

// The receiver reads a blocking socket, as the on-target RX task does, and its executor queue is full before the step
// runs, so the step's post of the feed meets a full queue.
struct full_queue_rig
{
    full_queue_rig()
            : filled(0)
            , ex()
            , pair(plexus::test::make_loopback_pair())
            , sender(std::move(pair.value().dialed))
            , receiver(blocking(std::move(pair.value().accepted)), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"})
            , ctx{receiver, ex, 0}
    {
        fill_queue(ex, filled);
    }

    std::uint32_t filled;
    freertos_executor ex;
    std::optional<plexus::test::loopback_pair> pair;
    host_tcp_socket sender;
    lwip_channel receiver;
    plexus::freertos::detail::lwip_rx_ctx<host_tcp_socket> ctx;
};

const std::vector<std::byte> k_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};

}

TEST_CASE("lwip rx task waits for queue room and delivers the feed once the executor drains", "[seam]")
{
    full_queue_rig rig;
    std::vector<std::byte> received;
    std::int32_t errors = 0;
    rig.receiver.on_data([&received](std::span<const std::byte> bytes) { received.assign(bytes.begin(), bytes.end()); });
    rig.receiver.on_error([&errors](plexus::io::io_error) { ++errors; });
    rig.sender.send(on_wire(k_payload));

    bool open = false;
    std::thread rx([&] { open = plexus::freertos::detail::lwip_rx_step(rig.ctx); });
    CHECK(rx_task_parked());
    plexus::test::poll_until([&] { rig.ex.pump(); }, [&] { return !received.empty(); });
    rx.join();
    rig.ex.drain();

    CHECK(received == on_wire(k_payload));
    CHECK(errors == 0);
    CHECK(open);
    CHECK(rig.filled == freertos_executor::k_queue_depth);
    CHECK(acquirable_slots(rig.receiver) == lwip_channel::k_rx_slots);
}

TEST_CASE("lwip rx task: closing a channel releases a receive task waiting for queue room", "[seam][lwip_rx_task]")
{
    plexus::test::rx_rig rig;
    std::uint32_t filled    = 0;
    std::int32_t deliveries = 0;
    std::int32_t closed     = 0;
    fill_queue(rig.ex, filled);
    rig.channel->on_data([&deliveries](std::span<const std::byte>) { ++deliveries; });
    rig.channel->on_closed([&closed] { ++closed; });
    REQUIRE(rig.spawn() == pdTRUE);
    rig.sender.send(on_wire(k_payload));

    std::atomic<bool> done{false};
    std::thread task(
            [&done]
            {
                plexus::test::run_spawned_task();
                done = true;
            });
    const std::thread::id task_id = task.get_id();
    CHECK(rx_task_parked());
    rig.channel->close();
    plexus::test::poll_until([&rig] { rig.ex.pump(); }, [&done] { return done.load(); });
    task.join();
    rig.ex.drain();

    CHECK(deliveries == 0);
    CHECK(closed == 1);
    CHECK(acquirable_slots(*rig.channel) == lwip_channel::k_rx_slots);
    CHECK(rig.closer.load() == task_id);
}
