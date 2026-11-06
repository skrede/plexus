#include "lwip_rx_rig.h"

#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <cstdint>
#include <utility>
#include <stdexcept>

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

TEST_CASE("lwip rx task: destroying an executor with a live receive task fails closed", "[seam][lwip_rx_task]")
{
    const auto leave_with_live_task = []
    {
        plexus::freertos::freertos_executor ex;
        auto pair = plexus::test::make_loopback_pair();
        REQUIRE(pair.has_value());
        rx_channel channel(std::move(pair->accepted), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"});
        REQUIRE(plexus::freertos::spawn_lwip_rx_task(channel, ex, plexus::freertos::task_options{4096}) == pdTRUE);
    };

    REQUIRE_THROWS_AS(leave_with_live_task(), std::runtime_error);
    plexus::test::release_spawned_ctx();
}
