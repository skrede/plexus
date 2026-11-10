#include "support/lane_forwarder.h"
#include "support/order_forwarder.h"

#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/detail/egress_scheduler.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <span>
#include <array>
#include <deque>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::testing::as_bytes;
using plexus::testing::arrival_log;
using plexus::testing::order_channel;
using plexus::testing::lane_forwarder;

namespace {

// Co-host subscribers' wire channels; every wire frame lands in one log apart from the ring's.
struct readers
{
    explicit readers(std::uint64_t count)
            : occupancy(0)
    {
        for(std::uint64_t key = 1; key <= count; ++key)
            wires.emplace_back(log, occupancy, key);
    }

    std::size_t occupancy;
    arrival_log log;
    std::deque<order_channel> wires;
};

arrival_log numbered(std::uint32_t from, std::uint32_t to)
{
    arrival_log out;
    for(std::uint32_t n = from; n <= to; ++n)
        out.push_back(std::to_string(n));
    return out;
}

}

TEST_CASE("write_once one publish writes the lane once for one, two or three ready readers, with or without the self-route", "[shm][write_once]")
{
    const std::uint64_t count = GENERATE(1u, 2u, 3u);
    const bool self           = GENERATE(false, true);
    lane_forwarder p;
    readers r{count};
    for(std::uint64_t i = 0; i < count; ++i)
        p.subscribe(r.wires[i], "peer-" + std::to_string(i));
    if(self)
        p.attach_self();

    p.publish(5);

    REQUIRE(p.ring_log == numbered(1, 5));
    REQUIRE(r.log.empty());
}

TEST_CASE("write_once a publish writes the lane once for the self-route alone and never with no ready reader", "[shm][write_once]")
{
    const bool self = GENERATE(false, true);
    lane_forwarder p;
    readers r{1};
    p.subscribe(r.wires[0], "peer-0", false);
    if(self)
        p.attach_self();

    p.publish(3);

    REQUIRE(p.ring_log == (self ? numbered(1, 3) : arrival_log{}));
    REQUIRE(r.log == numbered(1, 3));
}

TEST_CASE("write_once an object publish writes the lane once for the node and a ready reader", "[shm][write_once]")
{
    lane_forwarder p;
    readers r{1};
    p.subscribe(r.wires[0], "peer-0");
    p.attach_self();

    const std::string body = "7";
    p.fwd.publish_object("t", plexus::io::object_carrier{}, [&] { return as_bytes(body); });

    REQUIRE(p.ring_log == arrival_log{"7"});
    REQUIRE(r.log.empty());
}

TEST_CASE("write_once frames queued toward a remaining reader survive the self-route's departure", "[shm][write_once]")
{
    lane_forwarder p;
    readers r{1};
    p.subscribe(r.wires[0], "peer-0");
    p.attach_self();
    p.ring_occupancy = plexus::io::detail::k_low_water + 1;
    p.publish(3);
    REQUIRE(p.ring_log.empty());

    p.fwd.detach_local("t", *p.lanes.self_channel("t"));
    p.lanes.release_self("t");
    p.ring_occupancy = 0;
    p.fwd.publish("t", as_bytes("4"));

    REQUIRE(p.ring_log == numbered(1, 4));
}

TEST_CASE("write_once a lost lane leaves no fan-out entry and no self-route behind", "[shm][write_once]")
{
    lane_forwarder p;
    p.attach_self();
    REQUIRE(p.lanes.lane_for("t") != nullptr);

    p.lanes.on_medium_lost("t");
    p.publish(2);

    REQUIRE(p.lanes.lane_for("t") == nullptr);
    REQUIRE(p.ring_log.empty());
    REQUIRE_FALSE(p.fwd.has_remote_demand(plexus::wire::fqn_topic_hash("t"), nullptr));
}

TEST_CASE("write_once a self-route's latched replay reaches only the node, never the lane its readers share", "[shm][write_once]")
{
    lane_forwarder p{plexus::topic_qos{.latch = true, .depth = 1}};
    readers r{1};
    p.subscribe(r.wires[0], "peer-0");
    p.publish(1);
    REQUIRE(p.ring_log == numbered(1, 1));

    std::vector<std::vector<std::byte>> replayed;
    const plexus::io::subscriber_qos latest{.durability_mode = plexus::io::durability::latest};
    p.fwd.attach_local("t", *p.lanes.hold_self("t"), "self", latest, std::nullopt, [&](std::span<const std::byte> frame) { replayed.emplace_back(frame.begin(), frame.end()); });

    REQUIRE(replayed.size() == 1);
    REQUIRE(plexus::testing::arrival_of(replayed.front()) == "1");
    REQUIRE(p.ring_log == numbered(1, 1));
}
