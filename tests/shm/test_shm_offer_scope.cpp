#include "support/hold_probe.h"
#include "support/co_host_node.h"

#include "plexus/shm/region_naming.h"

#include "plexus/io/upgrade_channel.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <optional>

using plexus::testing::settle;
using plexus::testing::index_of;
using plexus::testing::step_both;
using plexus::testing::ring_probe;
using plexus::testing::index_bytes;
using plexus::testing::co_host_node;
using plexus::testing::hinted_topic;
using plexus::testing::co_host_region;
using plexus::testing::hinted_subscriber;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t k_messages = 16;

bool owns_ring(co_host_node &n, const std::string &fqn)
{
    return n.shm.registry().channel_for(fqn, plexus::shm::ring_direction::request, n.node.id()) != nullptr;
}

std::uint64_t not_offered(const co_host_node &n)
{
    return n.node.same_host_fallbacks()[static_cast<std::size_t>(plexus::io::fallback_reason::not_offered)];
}

std::vector<std::uint32_t> first_indices(std::uint32_t count)
{
    std::vector<std::uint32_t> indices(count);
    std::iota(indices.begin(), indices.end(), 0u);
    return indices;
}

}

// A node that publishes and subscribes to a topic subscribes to it at every co-host peer, so a node that only subscribes
// hears a subscribe for a topic it does not publish.
TEST_CASE("offer_scope a node with no publisher for a topic mints no ring for a co-host subscriber", "[shm][offer_scope]")
{
    const std::string region = co_host_region("osc");
    const std::string fqn    = "offer_scope.unpublished";
    co_host_node publisher{"publisher", 0x61, region};
    co_host_node subscriber{"subscriber", 0x62, region};
    plexus::publisher<> pub{publisher.node, fqn, hinted_topic()};
    plexus::subscriber<> own{publisher.node, fqn, hinted_subscriber(), [](std::span<const std::byte>) {}};
    plexus::subscriber<> sub{subscriber.node, fqn, hinted_subscriber(), [](std::span<const std::byte>) {}};

    publisher.node.listen({"tcp", "127.0.0.1:0"});
    subscriber.node.dial({"tcp", "127.0.0.1:" + std::to_string(publisher.remote.port())});
    REQUIRE(step_both(publisher, subscriber, [&] { return subscriber.reads_ring_of(publisher, fqn) && subscriber.node.count_subscribers(fqn) == 2; }, 5s));

    REQUIRE(owns_ring(publisher, fqn));
    REQUIRE_FALSE(owns_ring(subscriber, fqn));
}

// A node with no publisher answers the subscribe without a lane, and only the publisher declared later offers its ring.
// The publisher has no subscriber of its own, so its ring carries only what it routes to the switched subscriber.
TEST_CASE("offer_scope a publisher declared after a co-host subscriber subscribed engages shared memory", "[shm][offer_scope]")
{
    const std::string region = co_host_region("osl");
    const std::string fqn    = "offer_scope.late";
    co_host_node publisher{"publisher", 0x63, region};
    co_host_node subscriber{"subscriber", 0x64, region};
    std::vector<std::uint32_t> seen;
    plexus::subscriber<> sub{subscriber.node, fqn, hinted_subscriber(), [&seen](std::span<const std::byte> bytes) { seen.push_back(index_of(bytes)); }};
    publisher.node.listen({"tcp", "127.0.0.1:0"});
    subscriber.node.dial({"tcp", "127.0.0.1:" + std::to_string(publisher.remote.port())});
    REQUIRE(step_both(publisher, subscriber, [&] { return publisher.node.count_subscribers(fqn) == 1; }, 5s));
    settle(publisher, subscriber);
    REQUIRE_FALSE(owns_ring(publisher, fqn));
    REQUIRE(not_offered(subscriber) == 0);

    std::optional<plexus::publisher<>> pub{std::in_place, publisher.node, fqn, hinted_topic()};
    REQUIRE(step_both(publisher, subscriber, [&] { return subscriber.reads_ring_of(publisher, fqn); }, 5s));
    settle(publisher, subscriber);
    ring_probe ring;
    REQUIRE(ring.attach(region, fqn, publisher.node.id()));
    const std::uint64_t before = ring.tail();
    for(std::uint32_t index = 0; index < k_messages; ++index)
        pub->publish(index_bytes(index));
    REQUIRE(step_both(publisher, subscriber, [&] { return seen.size() >= k_messages; }, 5s));
    settle(publisher, subscriber);

    REQUIRE(seen == first_indices(k_messages));
    REQUIRE(ring.tail() - before == k_messages);
    REQUIRE(not_offered(subscriber) == 0);
}
