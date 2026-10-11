#include "sem_support.h"
#include "region_doorbells.h"

#include "support/heap_ring.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/slot_subscriber.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <thread>
#include <cstdint>
#include <utility>
#include <optional>

namespace {

using namespace shm_harness;

constexpr std::uint32_t k_iterations = 100;

// One relay handler runs and queues a drain, then the notifier is destroyed before that drain runs.
bool destroy_with_pending_drain(plexus::testing::heap_ring &heap, const std::string &region, std::uint32_t slot)
{
    ::asio::io_context io;
    std::uint32_t drains = 0;
    std::optional<sem_notifier> notifier(std::in_place, io, heap.ring().header(), region);
    if(notifier->arm(slot, [&drains] { ++drains; }) != plexus::shm::arm_status::armed || !wait_parked(heap.ring().header(), slot))
        return false;

    std::thread signaler([&notifier] { notifier->signal(); });
    signaler.join();
    const bool relayed = wait_parked(heap.ring().header(), slot) && io.run_one_for(std::chrono::seconds(1)) == 1;
    notifier.reset();
    io.poll();
    return relayed && drains == 0;
}

}

TEST_CASE("sem_pending a notifier destroyed with a wake pending touches no freed state", "[shm][sem_pending]")
{
    const std::string region = plexus::shm::region_name_for("shm_harness.sem_pending." + std::to_string(plexus::testing::process_id()), plexus::shm::ring_direction::request);
    const region_doorbells doorbells{region};
    plexus::testing::heap_ring heap{16, 64};
    plexus::shm::slot_subscriber subscriber{heap.ring()};
    REQUIRE(heap.created());
    REQUIRE(subscriber.cursor_index());

    std::uint32_t pending = 0;
    for(std::uint32_t iteration = 0; iteration < k_iterations; ++iteration)
        pending += destroy_with_pending_drain(heap, region, *subscriber.cursor_index()) ? 1 : 0;

    REQUIRE(pending == k_iterations);
}
