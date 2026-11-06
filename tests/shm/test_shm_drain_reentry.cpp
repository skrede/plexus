#include "support/marker_lane.h"
#include "support/co_host_node.h"

#include "plexus/shm/loan_status.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::shm::loan_status;
using plexus::testing::settle;
using plexus::testing::index_of;
using plexus::testing::step_both;
using plexus::testing::index_bytes;
using plexus::testing::co_host_node;
using plexus::testing::hinted_topic;
using plexus::testing::marker_lane;
using plexus::testing::co_host_region;
using plexus::testing::hinted_subscriber;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t k_queued = 4;

template<typename Publisher>
void publish_range(Publisher &pub, std::uint32_t first, std::uint32_t count)
{
    for(std::uint32_t index = first; index < first + count; ++index)
        pub.publish(index_bytes(index));
}

// The receive companion takes one message, then releases itself from inside the sink the drain is running.
void resume_releasing(marker_lane &lane)
{
    lane.receive->resume(
            [&lane](std::span<const std::byte> bytes)
            {
                lane.delivered.push_back(index_of(bytes));
                lane.receive.reset();
            });
}

void publish_queued(marker_lane &lane)
{
    for(std::uint32_t index = 0; index < k_queued; ++index)
        REQUIRE(lane.publish(index) == loan_status::ok);
}

}

// The subscriber first takes a batch from the ring, so the publishes that follow are drained from the ring and the drop
// lands inside that drain.
TEST_CASE("drain_reentry a subscriber that drops its own handle inside its callback stops delivery and leaves the publisher publishing", "[shm][drain_reentry]")
{
    const std::string region = co_host_region("dre");
    const std::string fqn    = "drain_reentry.topic";
    co_host_node publisher{"publisher", 0x61, region};
    co_host_node subscriber{"subscriber", 0x62, region};
    plexus::publisher<> pub{publisher.node, fqn, hinted_topic()};
    std::uint32_t calls = 0;
    bool drop           = false;
    std::optional<plexus::subscriber<>> sub;
    sub.emplace(subscriber.node, fqn, hinted_subscriber(),
                [&](std::span<const std::byte>)
                {
                    ++calls;
                    if(drop)
                        sub.reset();
                });

    publisher.node.listen({"tcp", "127.0.0.1:0"});
    subscriber.node.dial({"tcp", "127.0.0.1:" + std::to_string(publisher.remote.port())});
    REQUIRE(step_both(publisher, subscriber, [&] { return subscriber.reads_ring_of(publisher, fqn); }, 5s));
    subscriber.run_for(20ms);
    publish_range(pub, 0, k_queued);
    settle(publisher, subscriber);
    REQUIRE(calls == k_queued);

    drop = true;
    publish_range(pub, k_queued, k_queued);
    settle(publisher, subscriber);
    CHECK(calls == k_queued + 1);
    CHECK_FALSE(sub);

    std::vector<std::uint32_t> seen;
    plexus::subscriber<> fresh{subscriber.node, fqn, hinted_subscriber(), [&seen](std::span<const std::byte> bytes) { seen.push_back(index_of(bytes)); }};
    REQUIRE(step_both(publisher, subscriber, [&] { return subscriber.reads_ring_of(publisher, fqn); }, 5s));
    subscriber.run_for(20ms);
    publish_range(pub, 2 * k_queued, 1);
    settle(publisher, subscriber);
    CHECK(seen == std::vector<std::uint32_t>{2 * k_queued});
    CHECK(calls == k_queued + 1);
}

TEST_CASE("drain_reentry a receive companion released inside its own sink closes the ring only after the drain returns", "[shm][drain_reentry]")
{
    marker_lane lane{"drain_reentry.closed"};
    lane.attach();
    lane.send.reset();
    resume_releasing(lane);
    publish_queued(lane);
    lane.pump();
    CHECK(lane.delivered == std::vector<std::uint32_t>{0});
    CHECK_FALSE(lane.receive);
    CHECK(lane.member.registry().live_count() == 0);
}

TEST_CASE("drain_reentry a receive companion released inside its own sink while the producer holds the ring leaves the producer sending", "[shm][drain_reentry]")
{
    marker_lane lane{"drain_reentry.producing"};
    lane.attach();
    resume_releasing(lane);
    publish_queued(lane);
    lane.pump();
    CHECK(lane.delivered == std::vector<std::uint32_t>{0});
    CHECK(lane.member.registry().live_count() == 1);
    for(std::uint32_t index = 0; index < lane.cells() + 1; ++index)
        REQUIRE(lane.publish(k_queued + index) == loan_status::ok);
}
