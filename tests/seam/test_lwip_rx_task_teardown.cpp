#include "lwip_rx_rig.h"

#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"

#include "plexus/io/detail/channel_graveyard.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <cstdint>
#include <utility>

using plexus::test::rx_rig;
using plexus::test::rx_channel;

namespace {

bool finishes(const std::atomic<bool> &done)
{
    return plexus::test::poll_until([] {}, [&done] { return done.load(); }, std::chrono::seconds{1});
}

}

TEST_CASE("lwip rx task: a peer close seen by the receive task fails the channel on the executor", "[seam][lwip_rx_task]")
{
    rx_rig rig;
    std::int32_t errors = 0;
    std::int32_t closes = 0;
    std::thread::id error_thread;
    rig.channel->on_error(
            [&errors, &error_thread](plexus::io::io_error)
            {
                ++errors;
                error_thread = std::this_thread::get_id();
            });
    rig.channel->on_closed([&closes] { ++closes; });
    REQUIRE(rig.spawn() == pdTRUE);

    rig.sender.close();
    std::atomic<bool> done{false};
    std::thread task(
            [&done]
            {
                plexus::test::run_spawned_task();
                done = true;
            });
    const bool finished = finishes(done);
    task.join();
    const std::int32_t errors_before_drain = errors;
    rig.ex.drain();

    CHECK(finished);
    CHECK(errors_before_drain == 0);
    CHECK(errors == 1);
    CHECK(error_thread == std::this_thread::get_id());
    CHECK(closes == 1);
    CHECK(rig.channel->closed());
    CHECK(rig.closer.load() == std::this_thread::get_id());
}

TEST_CASE("lwip rx task: a retired channel stays in its graveyard while its receive task is parked and leaves it once the task exits", "[seam][lwip_rx_task]")
{
    rx_rig rig;
    plexus::io::detail::channel_graveyard<rx_channel, 1> graveyard;
    REQUIRE(rig.spawn() == pdTRUE);

    graveyard.retire(std::move(rig.channel));
    rig.ex.drain();
    const bool held_after_drain = graveyard.size() == 1;
    plexus::test::run_spawned_task();
    rig.ex.drain();

    CHECK(held_after_drain);
    CHECK(graveyard.empty());
}
