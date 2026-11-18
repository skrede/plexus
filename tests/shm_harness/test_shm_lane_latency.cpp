#include "lane_probe.h"

#include <catch2/catch_test_macros.hpp>

using namespace shm_harness;

TEST_CASE("lane_latency the publisher's p99 send to a subscriber restarted after a crash stays within 1 ms", "[shm][lane_latency]")
{
    const crash_restart_outcome outcome = crash_restart("crash_restart_latency", 100);

    require_exactly(outcome.subscriber, 100);
    REQUIRE(outcome.publisher.p99_send_us <= 1000);
}
