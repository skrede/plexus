// The mixed topology: an intra-node self-route COMPOSED with a network transport on one node.
// Two witnessed properties — a multi-transport node self-delivers via the FRAMED bytes lane (the
// erased channel has no zero-copy object hop, so the codec encodes), and a node that both
// self-publishes AND receives from a remote peer delivers each physical message EXACTLY once (no
// double-delivery across the two entry points).

#include "test_self_loopback_mixed_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus_test_mixed::sample;
using plexus_test_mixed::mixed_net;
using plexus_test_mixed::counting_codec;

TEST_CASE("self_loopback_mixed: a multi-transport node self-delivers on the framed bytes lane (encodes)", "[node][loopback][mixed]")
{
    mixed_net n;

    std::vector<std::uint32_t> seen;
    counting_codec codec;
    auto encodes = codec.encodes;
    plexus::subscriber<counting_codec> s{n.a, "topic", [&](const sample &v) { seen.push_back(v.value); }};
    plexus::publisher<counting_codec> p{n.a, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    p.publish(sample{0x1234u});
    REQUIRE(seen.empty()); // posted, never synchronous
    n.drive();

    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == 0x1234u);
    // The erased polymorphic_byte_channel exposes no send_object, so the object fast path is
    // unavailable: self-delivery degrades to the framed bytes lane and the codec MUST have encoded.
    REQUIRE(encodes->load() > 0);
}

// Bytes (not typed) on purpose: the deterministic inproc bus reports scheme "inproc" on every
// channel, so a peer dialed over the bus is classified process-tier and the object fast path would
// fire — masking the wire path. The no-double-delivery property is transport-tier-agnostic; the
// bytes lane proves it cleanly while keeping the test single-process. (A genuine non-process remote
// for the typed object lane is the asio TCP path; the framed self-lane is covered in SECTION 1.)
TEST_CASE("self_loopback_mixed: a self-publish and a remote-publish each deliver exactly once", "[node][loopback][mixed]")
{
    constexpr int k_own    = 4;
    constexpr int k_remote = 3;
    const std::array<std::byte, 4> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

    mixed_net n;
    n.connect();

    int hits = 0;
    plexus::subscriber<> s{n.a, "topic", [&](std::span<const std::byte>) { ++hits; }};
    plexus::publisher<> own{n.a, "topic"};
    plexus::publisher<> remote{n.b, "topic"};
    n.drive();

    for(int i = 0; i < k_own; ++i)
        own.publish(payload);
    for(int i = 0; i < k_remote; ++i)
        remote.publish(payload);
    n.drive();

    // Each physical message arrives once: a self-published message enters via the self-route, a
    // remote-published one via the peer session — never both for one message.
    REQUIRE(hits == k_own + k_remote);
}
