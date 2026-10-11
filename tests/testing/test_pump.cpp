#include "plexus/testing/pump.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#if defined(PLEXUS_HAVE_ASIO)
    #include <asio/post.hpp>
    #include <asio/io_context.hpp>
#endif

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

using namespace std::chrono_literals;

#if defined(PLEXUS_HAVE_ASIO)

TEST_CASE("pump returns once a posted predicate holds", "[testing][pump]")
{
    ::asio::io_context io;
    bool ran = false;
    ::asio::post(io, [&] { ran = true; });

    REQUIRE(plexus::testing::pump_until(io, [&] { return ran; }));
}

TEST_CASE("pump gives up when the bound elapses", "[testing][pump]")
{
    ::asio::io_context io;
    const auto start = std::chrono::steady_clock::now();

    REQUIRE_FALSE(plexus::testing::pump_until(io, [] { return false; }, 20ms));
    REQUIRE(std::chrono::steady_clock::now() - start >= 20ms);
}

TEST_CASE("pump keeps running work posted after the executor stopped", "[testing][pump]")
{
    ::asio::io_context io;
    io.run();
    REQUIRE(io.stopped());

    bool ran = false;
    ::asio::post(io, [&] { ran = true; });

    REQUIRE(plexus::testing::pump_until(io, [&] { return ran; }, 1s));
}

#endif

TEST_CASE("steps stop when no work remains", "[testing][pump]")
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    std::int32_t ran = 0;
    for(std::int32_t i = 0; i < 3; ++i)
        ex.post([&] { ++ran; });

    REQUIRE_FALSE(plexus::testing::pump_steps_until(ex, [&] { return ran == 10; }, 1000));
    REQUIRE(ran == 3);

    for(std::int32_t i = 0; i < 3; ++i)
        ex.post([&] { ++ran; });
    REQUIRE_FALSE(plexus::testing::pump_steps_until(ex, [] { return false; }, 2));
    REQUIRE(ran == 5);
    REQUIRE(plexus::testing::pump_steps_until(ex, [&] { return ran == 6; }, 1000));
}
