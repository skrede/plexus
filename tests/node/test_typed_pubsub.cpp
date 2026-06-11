// Typed pub/sub over the node facade, two nodes on a shared inproc bus + static_discovery
// (single-dialer, copied from the bytes fixture). It proves:
//   - the byte fallback round-trip: a bytes publisher sends a codec-encoded frame on the
//     typed topic and the typed subscriber decodes it to a value-equal T;
//   - the zero-serialization fast path: a typed publisher's borrowed object reaches the
//     typed callback by the SAME ADDRESS, the codec's encode never invoked, with
//     from_intra_process true and the carrier sequence/timestamps populated;
//   - the publish(const T&) convenience form over the fast path (one copy, zero encodes);
//   - the decode-failure default (drop + count, node stays up) and the opt-in escape
//     callback (raw bytes + errc instead);
//   - the strict posture refusing an undeclared bytes producer, lenient admitting;
//   - atomic retire: a dropped typed subscriber delivers nothing on a later publish.
// The codec is a hand-rolled trivial struct codec defined here — plexus never names a
// serializer library.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;
using plexus::io::message_info;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;
using bytes_publisher = plexus::publisher<inproc_policy>;

// A trivial value type: one 32-bit number. The codec serializes it little-endian and
// counts each encode so the fast-path zero-serialization witness is observable.
struct sample
{
    std::uint32_t value{};
};

struct counting_codec
{
    using value_type = sample;

    // The encode counter is shared so a publisher's encode activity is observable from the
    // test body regardless of which codec copy the endpoint holds.
    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const sample &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {0xABCD1234u, "sample"}; }
};

static_assert(plexus::typed_codec<counting_codec>);
static_assert(plexus::identity_bearing<counting_codec>);

using typed_publisher = plexus::publisher<inproc_policy, counting_codec>;
using typed_subscriber = plexus::subscriber<inproc_policy, counting_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0x7A9EDu;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::vector<std::byte> &b) { return {b.data(), b.size()}; }

std::vector<std::byte> encode_u32(std::uint32_t v)
{
    std::vector<std::byte> b(4);
    for(int i = 0; i < 4; ++i)
        b[i] = static_cast<std::byte>((v >> (8 * i)) & 0xff);
    return b;
}

// Only the subscriber node (A) dials eagerly; the publisher node (B) stays lazy (the
// single-dialer topology, copied from the bytes fixture).
struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

TEST_CASE("typed pub/sub: a bytes producer's encoded frame decodes to a value-equal T", "[node][typed][pubsub]")
{
    net n;
    n.connect();

    std::vector<sample> got;
    typed_subscriber s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
    // A bytes publisher on the SAME topic forces the byte path: the typed subscriber must
    // decode the wire frame (no shared in-process object), proving encode->bytes->decode
    // value equality end to end.
    bytes_publisher p{n.b, "topic"};
    n.drive();

    const auto frame = encode_u32(0xDEADBEEFu);
    p.publish(as_bytes(frame));
    n.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front().value == 0xDEADBEEFu);
}

TEST_CASE("typed pub/sub: the fast path delivers by address with zero encodes", "[node][typed][pubsub]")
{
    net n;
    n.connect();

    std::vector<const sample *> seen_addr;
    std::vector<std::uint32_t> seen_value;
    std::vector<message_info> infos;
    typed_subscriber s{n.a, "topic",
                       [&](const sample &v, const message_info &info) {
                           seen_addr.push_back(&v);
                           seen_value.push_back(v.value);
                           infos.push_back(info);
                       }};
    counting_codec codec;
    auto encodes = codec.encodes;
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    // Borrow, mark the object (mutate a field), capture its address, publish.
    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0x1234u;
    const sample *published_addr = &*loan;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(seen_value.size() == 1);
    REQUIRE(seen_value.front() == 0x1234u);
    // The zero-serialization witness: the subscriber observed the SAME object address the
    // publisher borrowed, and the codec's encode was never invoked.
    REQUIRE(seen_addr.front() == published_addr);
    REQUIRE(encodes->load() == 0);
    REQUIRE(infos.front().from_intra_process);
    REQUIRE(infos.front().reception_timestamp != 0);
    REQUIRE(infos.front().source_timestamp != 0);
    REQUIRE_FALSE(infos.front().source_identity.has_value());
}

TEST_CASE("typed pub/sub: publish(const T&) delivers value-equal over the fast path with zero encodes", "[node][typed][pubsub]")
{
    net n;
    n.connect();

    std::vector<std::uint32_t> got;
    typed_subscriber s{n.a, "topic", [&](const sample &v) { got.push_back(v.value); }};
    counting_codec codec;
    auto encodes = codec.encodes;
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    p.publish(sample{0x55AA55AAu});
    n.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == 0x55AA55AAu);
    REQUIRE(encodes->load() == 0);
    REQUIRE(p.loan_exhausted() == 0);
}

TEST_CASE("typed pub/sub: a decode failure is dropped and counted by default, escaped on opt-in", "[node][typed][pubsub]")
{
    SECTION("default: drop + count, node stays connected")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        typed_subscriber s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
        bytes_publisher p{n.b, "topic"};
        n.drive();

        const std::vector<std::byte> garbage(7, std::byte{0xFF});   // not 4 bytes -> decode fails
        p.publish(as_bytes(garbage));
        n.drive();

        REQUIRE(got.empty());
        REQUIRE(s.decode_failed() == 1);
        REQUIRE(n.a.router().is_connected(n.id_b));   // never a teardown
    }

    SECTION("opt-in escape callback receives the raw bytes + errc")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        std::vector<std::size_t> escaped_sizes;
        std::vector<std::error_code> escaped_errcs;
        plexus::typed_subscriber_options opts;
        opts.on_decode_failure = [&](std::span<const std::byte> raw, std::error_code ec) {
            escaped_sizes.push_back(raw.size());
            escaped_errcs.push_back(ec);
        };
        typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
        bytes_publisher p{n.b, "topic"};
        n.drive();

        const std::vector<std::byte> garbage(7, std::byte{0xFF});
        p.publish(as_bytes(garbage));
        n.drive();

        REQUIRE(got.empty());
        REQUIRE(s.decode_failed() == 1);
        REQUIRE(escaped_sizes.size() == 1);
        REQUIRE(escaped_sizes.front() == 7);
        REQUIRE(escaped_errcs.front() == std::make_error_code(std::errc::invalid_argument));
    }
}

TEST_CASE("typed pub/sub: strict posture refuses an undeclared bytes producer, lenient admits", "[node][typed][pubsub]")
{
    SECTION("strict: no delivery from an undeclared producer")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        plexus::typed_subscriber_options opts;
        opts.posture = plexus::io::attach_posture::strict;
        typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
        // A bytes publisher never declares a type_id -> undeclared producer.
        bytes_publisher p{n.b, "topic"};
        n.drive();

        const auto frame = encode_u32(0x11u);
        p.publish(as_bytes(frame));
        n.drive();

        REQUIRE(got.empty());   // strict refused the attach to an undeclared producer
    }

    SECTION("lenient default: admits an undeclared producer")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        typed_subscriber s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
        bytes_publisher p{n.b, "topic"};
        n.drive();

        const auto frame = encode_u32(0x22u);
        p.publish(as_bytes(frame));
        n.drive();

        REQUIRE(got.size() == 1);
        REQUIRE(got.front().value == 0x22u);
    }
}

TEST_CASE("typed pub/sub: a dropped typed subscriber delivers nothing on a later publish", "[node][typed][pubsub]")
{
    net n;
    n.connect();

    int delivered = 0;
    auto s = std::make_unique<typed_subscriber>(
        n.a, "topic", [&](const sample &) { ++delivered; });
    counting_codec codec;
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    p.publish(sample{1u});
    n.drive();
    REQUIRE(delivered == 1);

    // Drop the subscriber (atomic retire removes BOTH the decode adapter and the object
    // entry), then publish again — nothing reaches the dropped callback.
    s.reset();
    n.drive();
    p.publish(sample{2u});
    n.drive();
    REQUIRE(delivered == 1);
}
