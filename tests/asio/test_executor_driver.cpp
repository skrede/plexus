#include "plexus/asio/io_context_verbs.h"

#include "plexus/asio/detail/executor_driver.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <latch>
#include <atomic>
#include <thread>

namespace {

using verbs = plexus::asio::io_context_verbs;

struct observation
{
    std::atomic<bool> drives{false};
    std::atomic<std::thread::id> thread{};
};

void observe_driver(::asio::io_context &io, observation &into)
{
    into.drives = verbs::running_in_this_thread(io);
    into.thread = std::this_thread::get_id();
}

void run_on_two_threads(::asio::io_context &io)
{
    std::thread runner_a{[&] { io.run(); }};
    std::thread runner_b{[&] { io.run(); }};
    runner_a.join();
    runner_b.join();
}

}

TEST_CASE("executor_driver keeps an independent driver record per io_context", "[asio][executor_driver]")
{
    ::asio::io_context first;
    ::asio::io_context second;
    CHECK(&::asio::use_service<plexus::asio::detail::executor_driver>(first) != &::asio::use_service<plexus::asio::detail::executor_driver>(second));

    REQUIRE(verbs::running_in_this_thread(first));
    std::atomic<bool> worker_drives_second{false};
    std::thread worker{[&] { worker_drives_second = verbs::running_in_this_thread(second); }};
    worker.join();

    CHECK(worker_drives_second);
    CHECK(verbs::running_in_this_thread(first));
    CHECK_FALSE(verbs::running_in_this_thread(second));
}

TEST_CASE("executor_driver hands over from the setup thread to the thread that runs the io_context", "[asio][executor_driver]")
{
    ::asio::io_context io;
    REQUIRE(verbs::running_in_this_thread(io));

    std::atomic<bool> runner_drives{false};
    ::asio::post(io, [&] { runner_drives = verbs::running_in_this_thread(io); });
    std::thread runner{[&] { io.run(); }};
    runner.join();

    CHECK(runner_drives);
    CHECK_FALSE(verbs::running_in_this_thread(io));
}

TEST_CASE("executor_driver rejects a second thread running the same io_context inside a handler", "[asio][executor_driver]")
{
    ::asio::io_context io;
    observation first;
    observation second;
    std::latch second_done{1};
    auto observe_second = [&]
    {
        observe_driver(io, second);
        second_done.count_down();
    };
    auto observe_first = [&]
    {
        observe_driver(io, first);
        ::asio::post(io, observe_second);
        second_done.wait();
    };
    ::asio::post(io, observe_first);
    run_on_two_threads(io);

    REQUIRE(first.thread.load() != second.thread.load());
    CHECK(first.drives);
    CHECK_FALSE(second.drives);
}

TEST_CASE("executor_driver reports inside_executor only inside a handler of that io_context on this thread", "[asio][executor_driver]")
{
    ::asio::io_context io;
    ::asio::io_context other;
    std::atomic<bool> inside_own{false};
    std::atomic<bool> inside_other{true};
    std::latch entered{1};
    std::latch checked{1};
    auto probe = [&]
    {
        inside_own   = verbs::inside_executor(io);
        inside_other = verbs::inside_executor(other);
        entered.count_down();
        checked.wait();
    };
    ::asio::post(io, probe);
    std::thread runner{[&] { io.run(); }};
    entered.wait();
    CHECK_FALSE(verbs::inside_executor(io));
    checked.count_down();
    runner.join();
    CHECK(inside_own);
    CHECK_FALSE(inside_other);
    CHECK_FALSE(verbs::inside_executor(io));
}

TEST_CASE("executor_driver post_from_thread runs the work on the thread running the io_context", "[asio][executor_driver]")
{
    ::asio::io_context io;
    auto guard = ::asio::make_work_guard(io);
    std::atomic<std::thread::id> ran_on;
    std::thread runner{[&] { io.run(); }};
    const std::thread::id runner_id = runner.get_id();

    std::thread foreign{[&] { verbs::post_from_thread(io, [&] { ran_on = std::this_thread::get_id(); }); }};
    foreign.join();
    guard.reset();
    runner.join();

    CHECK(ran_on.load() == runner_id);
}
