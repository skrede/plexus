// After kill_origin the relay must not redial the origin's serial line: the line is freed with the origin, and the kernel
// may hand its pts name to a terminal another test owns.

#include "test_forward_serial_e2e_common.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

using namespace forward_serial_e2e_fixture;

namespace {

constexpr std::int32_t k_runs = 3;

// The first backoff is uniform in 0 to 400 ms over the fixture's 200 ms minimum delay, so this outlasts all of it.
constexpr std::chrono::milliseconds k_past_first_backoff{600};

void run_killed_origin_line_never_redialed()
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    cluster.bring_up_tcp();
    const std::optional<node_id> slot = cluster.relay->router().registry().id_for_endpoint(cluster.serial_line);
    REQUIRE(slot.has_value());
    const std::uint32_t attempts = cluster.relay->router().attempt_count(*slot);

    cluster.kill_origin();
    cluster.pump([&] { return !connected(*cluster.relay, cluster.origin_id); });
    serial_fixture::settle(cluster.io, k_past_first_backoff);
    REQUIRE(cluster.relay->router().attempt_count(*slot) == attempts);
}

}

TEST_CASE("a relay whose serial origin was killed arms no redial to the freed line, over cold runs", "[integration][serial][relay][e2e][kill_origin]")
{
    for(std::int32_t run = 0; run < k_runs; ++run)
    {
        INFO("cold run " << run);
        run_killed_origin_line_never_redialed();
    }
}
