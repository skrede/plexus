#include "notifier_legs.h"
#include "support/heap_ring.h"

#include "plexus/asio/shm/linux/futex_notifier.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/fallback_notifier.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <thread>
#include <cstdint>

namespace {

using plexus::asio::asio_policy;
using plexus::shm::arm_status;
using plexus::shm::fallback_tier;
using plexus::testing::take_all;
using threaded_leg = plexus::asio::shm::ring_notifier_threaded<asio_policy>;

}

TEST_CASE("fallback_notifier a denied primary falls back to the threaded leg and drains", "[shm][fallback_notifier]")
{
    ::asio::io_context io;
    plexus::testing::heap_ring heap{16, 64};
    REQUIRE(heap.created());
    plexus::shm::slot_subscriber sub(heap.ring());
    plexus::shm::fallback_notifier<plexus::testing::denied_primary, threaded_leg> notifier(io, heap.ring().header(), "");

    std::uint32_t drained = 0;
    REQUIRE(notifier.arm(*sub.cursor_index(), [&] { drained += take_all(sub); }) == arm_status::armed);
    CHECK(notifier.engaged() == fallback_tier::secondary);
    CHECK(notifier.primary_status() == arm_status::denied);

    bool committed = false;
    std::thread producer([&] { committed = heap.commit_one(); });
    producer.join();
    CHECK(committed);
    CHECK(plexus::testing::pump_until(io, [&] { return drained == 1; }));
    notifier.disarm();
}

TEST_CASE("fallback_notifier a runtime demotion moves to the threaded leg without losing a message", "[shm][fallback_notifier]")
{
    ::asio::io_context io;
    plexus::testing::heap_ring heap{16, 64};
    REQUIRE(heap.created());
    plexus::shm::slot_subscriber sub(heap.ring());
    plexus::shm::fallback_notifier<plexus::testing::demoting_primary<asio_policy>, threaded_leg> notifier(io, heap.ring().header(), "");

    std::uint32_t drained = 0;
    REQUIRE(notifier.arm(*sub.cursor_index(), [&] { drained += take_all(sub); }) == arm_status::armed);
    CHECK(notifier.engaged() == fallback_tier::primary);
    CHECK(notifier.primary_status() == arm_status::armed);
    REQUIRE(heap.commit_one());

    REQUIRE(plexus::testing::pump_until(io, [&] { return notifier.engaged() == fallback_tier::secondary; }));
    CHECK(notifier.primary_status() == arm_status::unsupported);
    CHECK(plexus::testing::pump_until(io, [&] { return drained == 1; }));

    bool committed = false;
    std::thread producer([&] { committed = heap.commit_one(); });
    producer.join();
    CHECK(committed);
    CHECK(plexus::testing::pump_until(io, [&] { return drained == 2; }));
    notifier.disarm();
}

TEST_CASE("fallback_notifier both legs denied reports denied with no leg engaged", "[shm][fallback_notifier]")
{
    plexus::shm::fallback_notifier<plexus::testing::denied_primary, plexus::testing::denied_floor> notifier;

    CHECK(notifier.arm(0, [] {}) == arm_status::denied);
    CHECK(notifier.engaged() == fallback_tier::none);
    CHECK(notifier.primary_status() == arm_status::denied);
}
