#include "plexus/io/capture_policy.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

using plexus::io::capture_fidelity;
using plexus::io::capture_policy;
using plexus::io::decimation_mode;
using plexus::io::topic_capture_rule;
using plexus::testing::alloc_count;
using plexus::testing::reset_alloc_count;

namespace {

constexpr std::uint64_t topic_hash = 0x5eedc0de;

}

TEST_CASE("capture_policy a payload rule's first count_n decision allocates nothing", "[io][capture_policy]")
{
    capture_policy policy;
    policy.set_topic(topic_hash, topic_capture_rule{.fidelity = capture_fidelity::payload, .mode = decimation_mode::count_n, .decimation = 4});

    reset_alloc_count();
    const bool admitted           = policy.wants_payload(topic_hash, 1);
    const std::size_t allocations = alloc_count();

    REQUIRE(allocations == 0);
    REQUIRE_FALSE(admitted);
}

TEST_CASE("capture_policy a payload rule's first time_window decision allocates nothing", "[io][capture_policy]")
{
    capture_policy policy;
    policy.set_topic(topic_hash, topic_capture_rule{.fidelity = capture_fidelity::payload, .mode = decimation_mode::time_window, .window_ns = 1000});

    reset_alloc_count();
    const bool admitted           = policy.wants_payload(topic_hash, 1);
    const std::size_t allocations = alloc_count();

    REQUIRE(allocations == 0);
    REQUIRE(admitted);
}
