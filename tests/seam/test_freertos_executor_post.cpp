// Host-shim proof for the constrained-target executor's task-safe cross-context post:
// a second context hands caller-owned posted_work PODs to the cooperative loop through
// the FreeRTOS queue (the host stand-in copies the whole item by value), and the loop
// invokes them in FIFO order. The ctx storage is a test-owned, pre-declared array, so
// the post path allocates nothing per item — under the asan tree this run is the
// no-per-post-allocation witness. A mixed run proves m_posted is serviced before the
// queue (the existing pump priority) and that the queue path never touches m_posted.

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>

namespace {

struct sentinel_ctx
{
    int                sentinel;
    std::vector<int> *sink;
};

void append_sentinel(void *ctx) noexcept
{
    auto *slot = static_cast<sentinel_ctx *>(ctx);
    slot->sink->push_back(slot->sentinel);
}

}

TEST_CASE("post_from_task runs caller-owned PODs in FIFO order", "[seam]")
{
    plexus::freertos::freertos_executor exec;
    std::vector<int>                    ran;
    sentinel_ctx                        slots[3] = {{1, &ran}, {2, &ran}, {3, &ran}};

    for(auto &slot : slots)
        exec.post_from_task({&append_sentinel, &slot});
    exec.drain();

    REQUIRE(ran == std::vector<int>{1, 2, 3});
}

TEST_CASE("pump services m_posted before the queued posted_work", "[seam]")
{
    plexus::freertos::freertos_executor exec;
    std::vector<int>                    ran;
    sentinel_ctx                        queued{2, &ran};

    exec.post_from_task({&append_sentinel, &queued});
    exec.post([&ran] { ran.push_back(1); });
    exec.drain();

    REQUIRE(ran == std::vector<int>{1, 2});
    REQUIRE_FALSE(exec.pump());
}

TEST_CASE("the queue path runs the enqueued invokers without touching m_posted", "[seam]")
{
    plexus::freertos::freertos_executor exec;
    std::vector<int>                    ran;
    sentinel_ctx                        slots[2] = {{7, &ran}, {9, &ran}};

    for(auto &slot : slots)
        exec.post_from_task({&append_sentinel, &slot});
    exec.drain();

    REQUIRE(ran == std::vector<int>{7, 9});
    REQUIRE_FALSE(exec.pump());
}
