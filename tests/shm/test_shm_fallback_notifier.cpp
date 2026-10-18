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
#include <vector>
#include <cstdint>

#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h>

namespace {

using plexus::asio::asio_policy;
using plexus::shm::arm_status;
using plexus::shm::fallback_tier;
using plexus::testing::take_all;
using threaded_leg = plexus::asio::shm::ring_notifier_threaded<asio_policy>;

// RLIMIT_NPROC bounds the user's tasks, so a zero limit refuses every new thread of this process; an exception that
// escapes the arm terminates the child.
arm_status arm_threaded_without_threads() noexcept
{
    ::asio::io_context io;
    plexus::testing::heap_ring heap{16, 64};
    plexus::shm::slot_subscriber sub(heap.ring());
    threaded_leg leg(io, heap.ring().header(), "");
    const ::rlimit none{0, 0};
    if(::setrlimit(RLIMIT_NPROC, &none) != 0)
        return arm_status::armed;
    return leg.arm(*sub.cursor_index(), [] {});
}

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

TEST_CASE("fallback_notifier each move to the secondary, at arm or at runtime, reports the primary's reason once", "[shm][fallback_notifier]")
{
    ::asio::io_context io;
    plexus::testing::heap_ring heap{16, 64};
    REQUIRE(heap.created());
    plexus::shm::slot_subscriber sub(heap.ring());
    std::vector<arm_status> reported;
    auto report = [&](arm_status primary) { reported.push_back(primary); };

    plexus::shm::fallback_notifier<plexus::testing::denied_primary, threaded_leg> at_arm(io, heap.ring().header(), "");
    REQUIRE(at_arm.arm(*sub.cursor_index(), [] {}, nullptr, report) == arm_status::armed);
    CHECK(reported == std::vector<arm_status>{arm_status::denied});
    at_arm.disarm();

    plexus::shm::fallback_notifier<plexus::testing::demoting_primary<asio_policy>, threaded_leg> at_runtime(io, heap.ring().header(), "");
    REQUIRE(at_runtime.arm(*sub.cursor_index(), [] {}, nullptr, report) == arm_status::armed);
    REQUIRE(plexus::testing::pump_until(io, [&] { return at_runtime.engaged() == fallback_tier::secondary; }));
    CHECK(reported == std::vector<arm_status>{arm_status::denied, arm_status::unsupported});
    at_runtime.disarm();
}

TEST_CASE("fallback_notifier a runtime demotion whose secondary cannot arm reports the loss once", "[shm][fallback_notifier]")
{
    ::asio::io_context io;
    plexus::testing::heap_ring heap{16, 64};
    REQUIRE(heap.created());
    plexus::shm::slot_subscriber sub(heap.ring());
    plexus::shm::fallback_notifier<plexus::testing::demoting_primary<asio_policy>, plexus::testing::denied_floor> notifier(io, heap.ring().header(), "");

    std::uint32_t told = 0;
    arm_status reason  = arm_status::armed;
    auto lost          = [&](arm_status r)
    {
        ++told;
        reason = r;
    };
    REQUIRE(notifier.arm(*sub.cursor_index(), [] {}, lost) == arm_status::armed);
    io.run();

    CHECK(told == 1);
    CHECK(reason == arm_status::denied);
    CHECK(notifier.engaged() == fallback_tier::none);
    CHECK(notifier.primary_status() == arm_status::unsupported);
}

TEST_CASE("fallback_notifier an inner composite that loses its waiter demotes the outer one to its secondary", "[shm][fallback_notifier]")
{
    using inner = plexus::shm::fallback_notifier<plexus::testing::demoting_primary<asio_policy>, plexus::testing::denied_floor>;
    ::asio::io_context io;
    plexus::testing::heap_ring heap{16, 64};
    REQUIRE(heap.created());
    plexus::shm::slot_subscriber sub(heap.ring());
    plexus::shm::fallback_notifier<inner, threaded_leg> notifier(io, heap.ring().header(), "");

    std::uint32_t drained = 0;
    REQUIRE(notifier.arm(*sub.cursor_index(), [&] { drained += take_all(sub); }) == arm_status::armed);
    REQUIRE(heap.commit_one());

    REQUIRE(plexus::testing::pump_until(io, [&] { return notifier.engaged() == fallback_tier::secondary; }));
    CHECK(notifier.primary_status() == arm_status::denied);
    CHECK(plexus::testing::pump_until(io, [&] { return drained == 1; }));
    notifier.disarm();
}

TEST_CASE("fallback_notifier both legs denied reports denied with no leg engaged", "[shm][fallback_notifier]")
{
    plexus::shm::fallback_notifier<plexus::testing::denied_primary, plexus::testing::denied_floor> notifier;

    CHECK(notifier.arm(0, [] {}) == arm_status::denied);
    CHECK(notifier.engaged() == fallback_tier::none);
    CHECK(notifier.primary_status() == arm_status::denied);
}

TEST_CASE("fallback_notifier a threaded leg whose thread cannot start reports exhausted", "[shm][fallback_notifier]")
{
    if(::geteuid() == 0)
        SKIP("root is exempt from RLIMIT_NPROC");
    const ::pid_t child = ::fork();
    REQUIRE(child >= 0);
    if(child == 0)
        ::_exit(arm_threaded_without_threads() == arm_status::exhausted ? 0 : 1);

    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status));
    CHECK(WEXITSTATUS(status) == 0);
}
