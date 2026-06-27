// Latched replay across the self-route: a publisher latches a value, then a local subscriber that
// joins AFTER the publish receives the latched value on its first drain turn — parity with the
// remote late-join semantics. Durability is REQUESTED (durability::latest); a default-qos
// subscriber requests no replay and gets nothing, the same gate the remote path applies.

#include "test_self_loopback_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/subscriber_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>

using plexus::io::durability;
using plexus::io::subscriber_qos;
using plexus_test::fixture;

TEST_CASE("self_loopback_latch: a late local subscriber receives the latched value", "[node][loopback][latch]")
{
    fixture f;

    plexus::publisher<> p{f.node(), "topic", plexus::topic_qos{.latch = true, .depth = 1}};
    const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    p.publish(payload);
    f.drive();

    // The subscriber joins AFTER the publish and REQUESTS durability — replay_if_latched, wired into
    // attach_local, routes the retained frame into the self-channel; the posted re-entry delivers it.
    std::vector<std::vector<std::byte>> seen;
    plexus::subscriber<> s{f.node(), "topic", subscriber_qos{.durability_mode = durability::latest}, [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
    REQUIRE(seen.empty()); // the replay is POSTED, never synchronous inside the attach
    f.drive();

    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == std::vector<std::byte>(payload.begin(), payload.end()));
}

TEST_CASE("self_loopback_latch: a late subscriber requesting no durability gets no replay", "[node][loopback][latch]")
{
    fixture f;

    plexus::publisher<> p{f.node(), "topic", plexus::topic_qos{.latch = true, .depth = 1}};
    const std::array<std::byte, 2> payload{std::byte{0xAB}, std::byte{0xCD}};
    p.publish(payload);
    f.drive();

    std::vector<std::vector<std::byte>> seen;
    plexus::subscriber<> s{f.node(), "topic", [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
    f.drive();

    // Default qos => durability::none => no replay (the same gate the remote path applies). The
    // witness the wiring honors the subscriber's choice, not an unconditional dump.
    REQUIRE(seen.empty());
}
