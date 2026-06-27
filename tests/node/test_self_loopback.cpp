// The single-node self-delivery slice: a node built with NO caller transport delivers its own
// publications to its own subscribers in-process. Three witnessed properties — the bytes lane
// (posted, correct payload, not-before-drain), the typed lane (zero-copy: same object by address,
// the codec's encode never invoked), and no wire subscribe emitted for the self-route.

#include "test_self_loopback_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/node_name.h"
#include "plexus/io/message_info.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using plexus::io::message_info;
using plexus_test::sample;
using plexus_test::fixture;
using plexus_test::make_id;
using plexus_test::counting_codec;

TEST_CASE("self_loopback: bytes lane: a node delivers its own publish to its own bytes subscriber, posted", "[node][loopback]")
{
    fixture f;

    std::vector<std::vector<std::byte>> seen;
    plexus::subscriber<> s{f.node(), "topic", [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
    plexus::publisher<> p{f.node(), "topic"};
    f.drive();

    const std::array<std::byte, 3> payload{std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    p.publish(payload);

    REQUIRE(seen.empty()); // delivery is POSTED, never synchronous inside publish()
    f.drive();

    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == std::vector<std::byte>(payload.begin(), payload.end()));
}

TEST_CASE("self_loopback: typed lane: same-node typed publish is zero-copy — same address, zero encodes", "[node][loopback]")
{
    fixture f;

    std::vector<const sample *> seen_addr;
    std::vector<std::uint32_t> seen_value;
    std::vector<message_info> infos;
    plexus::subscriber<counting_codec> s{f.node(), "topic", [&](const sample &v, const message_info &info)
                                         {
                                             seen_addr.push_back(&v);
                                             seen_value.push_back(v.value);
                                             infos.push_back(info);
                                         }};
    counting_codec codec;
    auto encodes = codec.encodes;
    plexus::publisher<counting_codec> p{f.node(), "topic", plexus::typed_publisher_options{}, codec};
    f.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value                  = 0xABCDu;
    const sample *published_addr = &*loan;

    p.publish(std::move(loan));
    REQUIRE(seen_value.empty()); // posted, not synchronous
    f.drive();

    REQUIRE(seen_value.size() == 1);
    REQUIRE(seen_value.front() == 0xABCDu);
    REQUIRE(seen_addr.front() == published_addr); // the SAME object, by address
    REQUIRE(encodes->load() == 0);                // the codec's encode was never invoked
    REQUIRE(infos.front().from_intra_process);
}

TEST_CASE("self_loopback: emits no wire subscribe for the self-route", "[node][loopback]")
{
    fixture f;

    std::vector<std::vector<std::byte>> seen;
    plexus::subscriber<> s{f.node(), "topic", [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
    plexus::publisher<> p{f.node(), "topic"};
    f.drive();

    // attach_local records NO remembered demand for the self name — the self-route never puts a
    // subscribe control frame onto the channel (the witness the local subscriber is wire-silent).
    REQUIRE(f.node().message_forwarder().remembered_topics(plexus::io::node_name_of(make_id(0x70))).empty());

    const std::array<std::byte, 2> payload{std::byte{0xAB}, std::byte{0xCD}};
    p.publish(payload);
    f.drive();

    // Exactly one delivery — a stray subscribe frame would have ridden the same channel and either
    // double-delivered or been dropped; the single clean delivery proves the self-route is data-only.
    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == std::vector<std::byte>(payload.begin(), payload.end()));
}
