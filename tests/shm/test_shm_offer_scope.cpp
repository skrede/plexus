#include "support/co_host_node.h"

#include "plexus/shm/region_naming.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <cstddef>

using plexus::testing::step_both;
using plexus::testing::co_host_node;
using plexus::testing::hinted_topic;
using plexus::testing::co_host_region;
using plexus::testing::hinted_subscriber;

namespace {

using namespace std::chrono_literals;

bool owns_ring(co_host_node &n, const std::string &fqn)
{
    return n.shm.registry().channel_for(fqn, plexus::shm::ring_direction::request, n.node.id()) != nullptr;
}

}

// A node that publishes and subscribes to a topic subscribes to it at every co-host peer, so a node that only subscribes
// hears a subscribe for a topic it does not publish.
TEST_CASE("offer_scope a node with no publisher for a topic mints no ring for a co-host subscriber", "[shm][offer_scope][!shouldfail]")
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
