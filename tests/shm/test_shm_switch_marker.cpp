#include "support/marker_lane.h"
#include "support/co_host_node.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"
#include "plexus/wire/data_frame.h"

#include "plexus/wire/detail/data_frame_codec.h"

#include "plexus/detail/node_upgrade_wiring.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <span>
#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::shm::loan_status;
using plexus::shm::cursor_holds_position;
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

constexpr std::uint32_t k_before_ready = 8;
constexpr std::uint32_t k_after_ready  = 8;

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

// The node's receive path, as the receive lane injects into it.
struct injected_frames
{
    void inject_upgrade_receive(std::string_view, std::span<const std::byte> frame)
    {
        sequences.push_back(plexus::wire::framed_sequence(frame).value_or(0));
    }

    std::vector<std::uint64_t> sequences;
};

std::vector<std::byte> data_frame(std::uint64_t sequence)
{
    std::vector<std::byte> frame;
    const std::array<std::byte, 4> payload{};
    plexus::wire::encode_unidirectional_frame_into(
            frame, plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
            plexus::wire::unidirectional_header{.source = plexus::wire::endpoint_source_type::publisher, .sequence = sequence, .topic_hash = 7}, payload);
    return frame;
}

}

// The subscriber's ready is left unread while the publisher writes its ring for its own subscriber, so the subscriber's
// receive companion is attached to a ring that already carries what the wire is still bringing it.
TEST_CASE("switch_marker a subscriber attaching while the publisher's own subscriber reads the ring receives each message once", "[shm][switch_marker]")
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

TEST_CASE("switch_marker a receive companion takes nothing and gates nothing before its marker", "[shm][switch_marker]")
{
    marker_lane lane{"switch_marker.held"};
    lane.attach();
    const std::uint32_t written = 2 * lane.cells();
    for(std::uint32_t index = 0; index < written; ++index)
        REQUIRE(lane.publish(index) == loan_status::ok);
    lane.pump();
    CHECK_FALSE(cursor_holds_position(lane.reader_position()));

    REQUIRE(lane.place());
    lane.publish(written);
    lane.resume();
    lane.pump();
    CHECK(lane.delivered == std::vector<std::uint32_t>{written - 1, written});
}

// The placement is the newest message the producer wrote, since the publish the switch happens on may already be in the
// ring; the switch marker skips anything below the marker's own message.
TEST_CASE("switch_marker a placed reader gates the ring from its placement and delivers from there in order", "[shm][switch_marker]")
{
    marker_lane lane{"switch_marker.placed"};
    lane.attach();
    constexpr std::uint32_t k_before = 5;
    for(std::uint32_t index = 0; index < k_before; ++index)
        REQUIRE(lane.publish(index) == loan_status::ok);
    REQUIRE(lane.place());
    for(std::uint32_t index = k_before; index < k_before + lane.cells() - 1; ++index)
        REQUIRE(lane.publish(index) == loan_status::ok);
    CHECK(lane.publish(k_before + lane.cells()) == loan_status::congested);

    lane.resume();
    lane.pump();
    REQUIRE(lane.delivered.size() == lane.cells());
    for(std::uint32_t i = 0; i < lane.delivered.size(); ++i)
        CHECK(lane.delivered[i] == k_before - 1 + i);
    CHECK(lane.publish(0) == loan_status::ok);
}

TEST_CASE("switch_marker a reader resumed with no placement, as the node's own read half is, delivers from its attach", "[shm][switch_marker]")
{
    marker_lane lane{"switch_marker.own"};
    lane.publish(0);
    lane.attach();
    lane.resume();
    lane.publish(1);
    lane.publish(2);
    lane.pump();
    CHECK(lane.delivered == std::vector<std::uint32_t>{1, 2});
}

TEST_CASE("switch_marker a resumed companion skips frames below the marker and delivers the rest in order", "[shm][switch_marker]")
{
    injected_frames engine;
    auto deliver = plexus::detail::deliver_from(engine, "publisher", 5);
    deliver(plexus::wire::encode_companion_switch({0, 5}));
    for(std::uint64_t sequence = 2; sequence < 10; ++sequence)
        deliver(data_frame(sequence));
    CHECK(engine.sequences == std::vector<std::uint64_t>{5, 6, 7, 8, 9});
}
