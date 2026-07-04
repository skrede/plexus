// The fallback-guarantee slice: a node with NO intra_node transport but WITH an shm member still
// self-delivers a same-node publish→subscribe — over a node-local shm self-ring. The carrier mints
// both halves of one join_live ring on one node; the read half re-enters the node's own dispatch
// (no peer session). Two witnessed properties: the framed-bytes delivery lands once with the correct
// payload, and exactly one callback fires per publish (no double-delivery).

#include "test_shm_self_carrier_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/subscriber_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>

using plexus_test::shm_fixture;

TEST_CASE("shm_self_carrier: a node with no intra-node transport self-delivers over the shm self-ring", "[node][shm][loopback]")
{
    shm_fixture f;

    std::vector<std::vector<std::byte>> seen;
    plexus::subscriber<> s{f.node, "topic", [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
    plexus::publisher<> p{f.node, "topic"};
    f.drive_until([] { return false; }, std::chrono::milliseconds{50}); // settle the subscribe/mint turn

    const std::array<std::byte, 3> payload{std::byte{0xA1}, std::byte{0xB2}, std::byte{0xC3}};
    p.publish(payload);

    f.drive_until([&] { return !seen.empty(); });

    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == std::vector<std::byte>(payload.begin(), payload.end()));
}

// The latch parity on the FALLBACK path: the demand-gated shm self-ring mints on the late
// subscriber's first-subscribe, and install_shm_self_carrier's attach_local replays the retained
// frame (the forwarder retains a latched value even with no subscribers), so a late durability-
// requesting subscriber gets the latched value over the shm self-ring — parity with the intra-node
// path. A confirmation, not a gap.
TEST_CASE("shm_self_carrier: a late durability-requesting subscriber gets the latched value", "[node][shm][loopback][latch]")
{
    shm_fixture f;

    plexus::publisher<> p{f.node, "topic", plexus::topic_qos{.latch = true, .depth = 1}};
    const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    p.publish(payload);
    f.drive_until([] { return false; }, std::chrono::milliseconds{50});

    std::vector<std::vector<std::byte>> seen;
    plexus::subscriber<> s{f.node, "topic", plexus::io::subscriber_qos{.durability_mode = plexus::io::durability::latest},
                           [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
    f.drive_until([&] { return !seen.empty(); }, std::chrono::milliseconds{200});

    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == std::vector<std::byte>(payload.begin(), payload.end()));
}

TEST_CASE("shm_self_carrier: exactly one callback per publish - no double-delivery", "[node][shm][loopback]")
{
    shm_fixture f;

    std::size_t count = 0;
    plexus::subscriber<> s{f.node, "topic", [&](std::span<const std::byte>) { ++count; }};
    plexus::publisher<> p{f.node, "topic"};
    f.drive_until([] { return false; }, std::chrono::milliseconds{50});

    const std::array<std::byte, 2> payload{std::byte{0x5A}, std::byte{0x4E}};
    p.publish(payload);
    f.drive_until([&] { return count >= 1; });
    REQUIRE(count == 1);

    p.publish(payload);
    f.drive_until([&] { return count >= 2; });
    REQUIRE(count == 2);
}
