// A host thread stands in for the receive task where a case needs it parked in recv; elsewhere the test thread runs the
// spawned trampoline itself, after the close, as the executor would see the task finish.

#include "lwip_rx_rig.h"
#include "host_tcp_listener.h"

#include "support/retire_drain.h"

#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/lwip_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <cstddef>
#include <cstdint>

using plexus::test::rx_rig;
using plexus::test::rx_channel;
using plexus::test::host_tcp_socket;

namespace {

constexpr std::array<std::byte, 4> k_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};

bool finishes(const std::atomic<bool> &done)
{
    return plexus::test::poll_until([] {}, [&done] { return done.load(); }, std::chrono::seconds{1});
}

struct idle_outcome
{
    std::int32_t turns;
    bool finished;
};

idle_outcome idle_turns_until_close(rx_channel &channel, plexus::freertos::freertos_executor &ex)
{
    std::atomic<std::int32_t> turns{0};
    std::atomic<bool> done{false};
    std::thread task(
            [&turns, &done]
            {
                while(plexus::freertos::detail::lwip_rx_step(plexus::test::spawned_ctx()))
                    ++turns;
                done = true;
            });
    std::this_thread::sleep_for(std::chrono::milliseconds{350});
    const std::int32_t idle_turns = turns.load();
    channel.close();
    const bool finished = finishes(done);
    task.join();
    ex.drain();
    plexus::test::release_spawned_ctx();
    return {idle_turns, finished};
}

void queue_one_feed(rx_rig &rig)
{
    rig.sender.send(plexus::test::on_wire(k_payload));
    const bool queued = plexus::test::poll_until([] { plexus::freertos::detail::lwip_rx_step(plexus::test::spawned_ctx()); },
                                                 [&rig] { return plexus::test::acquirable_slots(*rig.channel) < rx_channel::k_rx_slots; });
    REQUIRE(queued);
}

}

TEST_CASE("lwip rx task: closing a channel wakes a receive task parked in recv and the task closes the socket", "[seam][lwip_rx_task]")
{
    rx_rig rig;
    std::int32_t closed = 0;
    rig.channel->on_closed([&closed] { ++closed; });
    REQUIRE(rig.spawn() == pdTRUE);

    std::atomic<bool> done{false};
    std::thread task(
            [&done]
            {
                plexus::test::run_spawned_task();
                done = true;
            });
    const std::thread::id task_id = task.get_id();
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    rig.channel->close();
    const bool finished = finishes(done);
    task.join();
    rig.ex.drain();

    CHECK(finished);
    CHECK(rig.closer.load() == task_id);
    CHECK(closed == 1);
}

TEST_CASE("lwip rx task: on_closed is posted only after the receive task exits and its queued feeds ran", "[seam][lwip_rx_task]")
{
    rx_rig rig;
    REQUIRE(rig.spawn() == pdTRUE);
    queue_one_feed(rig);

    std::int32_t closed = 0;
    std::size_t slots   = 0;
    rig.channel->on_closed(
            [&]
            {
                ++closed;
                slots = plexus::test::acquirable_slots(*rig.channel);
            });
    rig.channel->close();
    plexus::test::run_spawned_task();
    rig.ex.drain();

    CHECK(closed == 1);
    CHECK(slots == rx_channel::k_rx_slots);
}

TEST_CASE("lwip rx task: a feed queued before the close never reaches a channel destroyed in on_closed", "[seam][lwip_rx_task]")
{
    rx_rig rig;
    REQUIRE(rig.spawn() == pdTRUE);
    queue_one_feed(rig);

    const auto outcome = plexus::testing::retire_and_drain(rig.channel,
                                                           [&rig]
                                                           {
                                                               plexus::test::run_spawned_task();
                                                               rig.ex.drain();
                                                           });

    CHECK(outcome.erased == 1);
    CHECK(outcome.empty);
    CHECK_FALSE(rig.ex.pump());
}

TEST_CASE("lwip rx task: an idle receive under the task policy parks for the receive timeout instead of spinning", "[seam][lwip_rx_task]")
{
    rx_rig rig;
    REQUIRE(rig.spawn() == pdTRUE);

    const auto [idle_turns, finished] = idle_turns_until_close(*rig.channel, rig.ex);

    CHECK(idle_turns >= 1);
    CHECK(idle_turns <= 5);
    CHECK(finished);
}

TEST_CASE("lwip rx task: an idle channel the transport adopted under the task policy parks in recv instead of spinning", "[seam][lwip_rx_task]")
{
    plexus::freertos::freertos_executor ex;
    plexus::freertos::lwip_transport<host_tcp_socket, plexus::test::host_tcp_listener, 1> transport{ex};
    transport.use_rx_task(plexus::freertos::task_options{4096});
    plexus::test::host_tcp_listener probe;
    REQUIRE_FALSE(probe.bind_and_listen(plexus::io::endpoint{"tcp", "127.0.0.1:0"}));
    const auto address = probe.local_endpoint();
    probe.close();
    transport.listen(address);
    std::unique_ptr<rx_channel> accepted;
    transport.on_accepted([&accepted](std::unique_ptr<rx_channel> ch) { accepted = std::move(ch); });

    host_tcp_socket client;
    REQUIRE_FALSE(client.connect(address));
    REQUIRE(plexus::test::poll_until([&transport] { transport.poll(); }, [&accepted] { return accepted != nullptr; }));
    const auto [idle_turns, finished] = idle_turns_until_close(*accepted, ex);
    CHECK(finished);

    CHECK(idle_turns >= 1);
    CHECK(idle_turns <= 5);
}
