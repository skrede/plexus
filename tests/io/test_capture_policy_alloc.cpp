#include "plexus/io/capture_policy.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>

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

TEST_CASE("capture_policy a topic declared before its first publish is decimated under the default rule and allocates nothing", "[io][capture_policy]")
{
    capture_policy policy;
    policy.set_default(topic_capture_rule{.fidelity = capture_fidelity::payload, .mode = decimation_mode::count_n, .decimation = 4});
    policy.declare_topic(topic_hash, std::nullopt);

    reset_alloc_count();
    std::int32_t admitted = 0;
    for(std::uint64_t now_ns = 1; now_ns <= 8; ++now_ns)
        if(policy.wants_payload(topic_hash, now_ns))
            ++admitted;
    const std::size_t allocations = alloc_count();

    REQUIRE(allocations == 0);
    REQUIRE(admitted == 2);
}
