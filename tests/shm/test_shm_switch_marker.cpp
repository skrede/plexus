#include "support/co_host_node.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::testing::index_of;
using plexus::testing::index_bytes;
using plexus::testing::co_host_node;
using plexus::testing::hinted_topic;
using plexus::testing::co_host_region;
using plexus::testing::hinted_subscriber;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t k_before_ready = 8;
constexpr std::uint32_t k_after_ready  = 8;

// Each node runs one turn at a time, so a case decides which node reads what and when.
template<typename Done>
bool step_both(co_host_node &a, co_host_node &b, Done done, std::chrono::milliseconds budget)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(!done())
    {
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
        a.run_for(1ms);
        b.run_for(1ms);
    }
    return true;
}

void settle(co_host_node &a, co_host_node &b)
{
    step_both(a, b, [] { return false; }, 300ms);
}

template<typename Publisher>
void publish_range(Publisher &pub, std::uint32_t first, std::uint32_t count)
{
    for(std::uint32_t index = first; index < first + count; ++index)
        pub.publish(index_bytes(index));
}

std::size_t distinct(const std::vector<std::uint32_t> &seen)
{
    return std::set<std::uint32_t>(seen.begin(), seen.end()).size();
}

}

// The subscriber's ready is left unread while the publisher writes its ring for its own subscriber, so the subscriber's
// receive companion is attached to a ring that already carries what the wire is still bringing it.
TEST_CASE("switch_marker a subscriber attaching while the publisher's own subscriber reads the ring receives each message once", "[shm][switch_marker][!shouldfail]")
{
    const std::string region = co_host_region("swm");
    const std::string fqn    = "switch_marker.topic";
    co_host_node publisher{"publisher", 0x51, region};
    co_host_node subscriber{"subscriber", 0x52, region};
    plexus::publisher<> pub{publisher.node, fqn, hinted_topic()};
    plexus::subscriber<> own{publisher.node, fqn, hinted_subscriber(), [](std::span<const std::byte>) {}};
    std::vector<std::uint32_t> seen;
    plexus::subscriber<> sub{subscriber.node, fqn, hinted_subscriber(), [&seen](std::span<const std::byte> bytes) { seen.push_back(index_of(bytes)); }};

    publisher.node.listen({"tcp", "127.0.0.1:0"});
    subscriber.node.dial({"tcp", "127.0.0.1:" + std::to_string(publisher.remote.port())});
    REQUIRE(step_both(publisher, subscriber, [&] { return subscriber.reads_ring_of(publisher, fqn); }, 5s));
    subscriber.run_for(20ms);

    publish_range(pub, 0, k_before_ready);
    settle(publisher, subscriber);
    publish_range(pub, k_before_ready, k_after_ready);
    settle(publisher, subscriber);

    REQUIRE(distinct(seen) == k_before_ready + k_after_ready);
    REQUIRE(seen.size() == distinct(seen));
}
