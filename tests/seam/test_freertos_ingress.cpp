#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>
#include <cstddef>
#include <cstdint>

namespace {

using plexus::freertos::freertos_executor;
using plexus::freertos::posted_work;

constexpr std::uint32_t k_depth = freertos_executor::k_queue_depth;

void count_up(void *ctx) noexcept
{
    ++*static_cast<std::uint32_t *>(ctx);
}

void fill_with_posts(freertos_executor &ex, std::uint32_t &ran)
{
    for(std::uint32_t i = 0; i < k_depth; ++i)
        REQUIRE(ex.post_from_thread([&ran] { ++ran; }));
}

constexpr std::uint32_t k_racing_posts = 2000;

struct race_tally
{
    std::uint32_t ran        = 0;
    std::uint32_t ran_inside = 0;
    std::atomic<std::uint32_t> refused{0};
    std::atomic<bool> posted_all{false};
};

void post_counted(freertos_executor &ex, race_tally &tally)
{
    for(std::uint32_t i = 0; i < k_racing_posts; ++i)
    {
        const bool accepted = ex.post_from_thread(
                [&ex, &tally]
                {
                    ++tally.ran;
                    tally.ran_inside += ex.inside_executor() ? 1u : 0u;
                });
        if(!accepted)
            tally.refused.fetch_add(1, std::memory_order_relaxed);
    }
    tally.posted_all.store(true, std::memory_order_release);
}

}

TEST_CASE("freertos_ingress work posted from another thread runs at the next pump", "[seam]")
{
    freertos_executor ex;
    std::thread::id ran_on{};
    bool accepted = false;

    std::thread foreign([&] { accepted = ex.post_from_thread([&] { ran_on = std::this_thread::get_id(); }); });
    foreign.join();

    REQUIRE(accepted);
    REQUIRE(ran_on == std::thread::id{});
    REQUIRE(ex.pump());
    REQUIRE(ran_on == std::this_thread::get_id());
    REQUIRE_FALSE(ex.pump());
    REQUIRE(ex.ingress_dropped() == 0);
}

TEST_CASE("freertos_ingress a full pool counts a drop and allocates nothing", "[seam]")
{
    freertos_executor ex;
    std::uint32_t ran = 0;
    fill_with_posts(ex, ran);
    plexus::detail::move_only_function<void()> overflow{[&ran] { ran += 100; }};

    const std::size_t before = plexus::testing::alloc_count();
    const bool accepted      = ex.post_from_thread(std::move(overflow));
    const std::size_t after  = plexus::testing::alloc_count();

    REQUIRE_FALSE(accepted);
    REQUIRE(after == before);
    REQUIRE(ex.ingress_dropped() == 1);
    ex.drain();
    REQUIRE(ran == k_depth);
    REQUIRE(ex.post_from_thread([&ran] { ++ran; }));
    ex.drain();
    REQUIRE(ran == k_depth + 1);
    REQUIRE(ex.ingress_dropped() == 1);
}

TEST_CASE("freertos_ingress a full queue frees the slot and counts a drop", "[seam]")
{
    freertos_executor ex;
    std::uint32_t task_ran = 0;
    for(std::uint32_t i = 0; i < k_depth; ++i)
        REQUIRE(ex.post_from_task(posted_work{&count_up, &task_ran}));
    REQUIRE_FALSE(ex.post_from_task(posted_work{&count_up, &task_ran}));

    std::uint32_t ran = 0;
    REQUIRE_FALSE(ex.post_from_thread([&ran] { ++ran; }));
    REQUIRE(ex.ingress_dropped() == 1);
    ex.drain();
    REQUIRE(task_ran == k_depth);
    REQUIRE(ran == 0);

    fill_with_posts(ex, ran);
    ex.drain();
    REQUIRE(ran == k_depth);
    REQUIRE(ex.ingress_dropped() == 1);
}

TEST_CASE("freertos_ingress inside_executor is true only inside pump on the pumping task", "[seam]")
{
    freertos_executor ex;
    bool inside_on_pumper  = false;
    bool driver_on_pumper  = false;
    bool inside_on_foreign = true;

    ex.post(
            [&]
            {
                inside_on_pumper = ex.inside_executor();
                driver_on_pumper = ex.running_in_this_thread();
                std::thread foreign([&] { inside_on_foreign = ex.inside_executor(); });
                foreign.join();
            });

    REQUIRE_FALSE(ex.inside_executor());
    ex.drain();
    REQUIRE(inside_on_pumper);
    REQUIRE(driver_on_pumper);
    REQUIRE_FALSE(inside_on_foreign);
    REQUIRE_FALSE(ex.inside_executor());
}

TEST_CASE("freertos_ingress posts racing the pumping task each run on it or count as a drop", "[seam]")
{
    freertos_executor ex;
    race_tally tally;

    std::thread foreign([&] { post_counted(ex, tally); });
    while(!tally.posted_all.load(std::memory_order_acquire))
        ex.pump();
    foreign.join();
    ex.drain();

    REQUIRE(tally.ran > 0);
    REQUIRE(tally.ran_inside == tally.ran);
    REQUIRE(tally.refused.load() == ex.ingress_dropped());
    REQUIRE(tally.ran + tally.refused.load() == k_racing_posts);
}
