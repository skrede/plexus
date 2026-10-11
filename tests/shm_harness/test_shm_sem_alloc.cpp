#include "sem_support.h"
#include "region_doorbells.h"

#include "support/heap_ring.h"
#include "support/alloc_counter.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/slot_subscriber.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>
#include <cstdint>

namespace {

constexpr std::uint32_t k_signals = 1000;

bool signal_parked_waiter(shm_harness::sem_notifier &notifier, const plexus::shm::control_header_t &header, std::uint32_t slot)
{
    if(!shm_harness::wait_parked(header, slot))
        return false;
    notifier.signal();
    return true;
}

}

// The reactor is never run, so the window counts the producer's signal path and the waiter's park cycle alone.
TEST_CASE("sem_alloc the signal path allocates nothing after the first wake", "[shm][sem_alloc]")
{
    const std::string region = plexus::shm::region_name_for("shm_harness.sem_alloc." + std::to_string(plexus::testing::process_id()), plexus::shm::ring_direction::request);
    const shm_harness::region_doorbells doorbells{region};
    plexus::testing::heap_ring heap{16, 64};
    plexus::shm::slot_subscriber subscriber{heap.ring()};
    REQUIRE(heap.created());
    REQUIRE(subscriber.cursor_index());
    const std::uint32_t slot                    = *subscriber.cursor_index();
    const plexus::shm::control_header_t &header = heap.ring().header();

    ::asio::io_context io;
    shm_harness::sem_notifier notifier{io, heap.ring().header(), region};
    REQUIRE(notifier.arm(slot, [] {}) == plexus::shm::arm_status::armed);
    REQUIRE(signal_parked_waiter(notifier, header, slot));

    const std::size_t before = plexus::testing::alloc_count();
    std::uint32_t woken      = 0;
    for(std::uint32_t signal = 0; signal < k_signals; ++signal)
        woken += signal_parked_waiter(notifier, header, slot) ? 1 : 0;
    const std::size_t after = plexus::testing::alloc_count();

    REQUIRE(woken == k_signals);
    REQUIRE(after - before == 0);
}
