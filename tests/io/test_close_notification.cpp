#include "plexus/io/detail/close_notification.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <vector>
#include <cstdint>
#include <utility>

using plexus::io::detail::close_notification;
using posted_queue = std::vector<plexus::detail::move_only_function<void()>>;

namespace {

void run_all(posted_queue &queue)
{
    for(auto &fn : std::exchange(queue, {}))
        fn();
}

}

TEST_CASE("close_notification posts its handler once however often it is notified", "[io][close_notification]")
{
    posted_queue queue;
    auto post = [&queue](auto fn) { queue.emplace_back(std::move(fn)); };
    close_notification notification;
    std::int32_t closes = 0;
    notification.install([&closes] { ++closes; });

    notification.notify(post);
    notification.notify(post);
    REQUIRE(queue.size() == 1);
    REQUIRE(closes == 0);
    run_all(queue);
    notification.install([&closes] { ++closes; });
    notification.notify(post);
    REQUIRE(queue.empty());
    REQUIRE(closes == 1);
}

TEST_CASE("close_notification posts nothing until a handler is installed", "[io][close_notification]")
{
    posted_queue queue;
    auto post = [&queue](auto fn) { queue.emplace_back(std::move(fn)); };
    close_notification notification;
    std::int32_t closes = 0;

    notification.notify(post);
    REQUIRE(queue.empty());
    notification.install([&closes] { ++closes; });
    notification.notify(post);
    run_all(queue);
    REQUIRE(closes == 1);
}

TEST_CASE("close_notification posts a closure that outlives the notification it came from", "[io][close_notification]")
{
    posted_queue queue;
    auto post           = [&queue](auto fn) { queue.emplace_back(std::move(fn)); };
    auto notification   = std::make_unique<close_notification>();
    std::int32_t closes = 0;
    notification->install([&closes] { ++closes; });

    notification->notify(post);
    notification.reset();
    run_all(queue);
    REQUIRE(closes == 1);
}
