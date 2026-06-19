// The typed↔bytes interop matrix: every boundary between a typed and a bytes endpoint
// (and a typed pair declaring incompatible identities) has a behavioral cell here, each
// asserting the OBSERVABLE outcome — never merely the absence of a crash. The cells split
// across this file and test_typed_fastpath.cpp; together they cover the full matrix:
//   1  typed pub  -> bytes sub          : the bytes callback receives the codec's encoding
//   2  bytes pub  -> typed sub (good)   : decoded T equals the source
//   3  bytes pub  -> typed sub (garbage): default drop+count, opt-in escape gets the junk
//   4  typed pub  -> typed sub (bytes)  : encode->wire->decode round-trip value equality
//   5  typed pub  -> typed sub (X vs Y) : declared mismatch REFUSED, no delivery, session up
//   8  typed pub  -> bytes sub (inproc) : the byte path taken, the encoding received
//   9  strict typed sub -> undeclared   : refused, no delivery, session up
//   10 tag match + native-key mismatch  : counted drop + warn, NO cast, NO typed callback
// Cells 6 (identity witness) and 7 (the looped fast/fallback flip) live in the fastpath
// file. The codec is a hand-rolled trivial struct codec — plexus never names a serializer.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
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

using inproc_node      = plexus::node<inproc_policy, inproc_transport<>>;
using bytes_publisher  = plexus::publisher<>;
using bytes_subscriber = plexus::subscriber<>;

struct sample
{
    std::uint32_t value{};
};

// A counting codec with an explicit wire type identity. The encode counter is shared so
// the test body observes a publisher's encode activity regardless of the codec copy the
// endpoint holds.
struct counting_codec
{
    using value_type = sample;

    std::uint64_t                     tag     = 0xABCD1234u;
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

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes,
                                                   sample                    &out) const
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

    plexus::type_identity type_info() const { return {tag, "sample"}; }
};

static_assert(plexus::typed_codec<counting_codec>);
static_assert(plexus::identity_bearing<counting_codec>);

using typed_publisher  = plexus::publisher<counting_codec>;
using typed_subscriber = plexus::subscriber<counting_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0x511CEu;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::vector<std::byte> &b)
{
    return {b.data(), b.size()};
}

std::vector<std::byte> encode_u32(std::uint32_t v)
{
    std::vector<std::byte> b(4);
    for(int i = 0; i < 4; ++i)
        b[i] = static_cast<std::byte>((v >> (8 * i)) & 0xff);
    return b;
}

// Single-dialer topology (subscriber node A eager, publisher node B lazy), copied from the
// bytes fixture — both-eager double-delivers on a shared bus.
struct net
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

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

TEST_CASE("typed interop cell 1: a typed publisher reaches a bytes subscriber with the codec's "
          "encoding",
          "[node][typed][interop]")
{
    // A is the publisher node (eager so its demand-less role still dials), B the bytes
    // subscriber — flip the eager flag so the bytes subscriber's node is the single dialer.
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};
    inproc_node        pub_node{ex, disc, make_id(0x0A), ta, make_opts(/*eager=*/false)};
    inproc_node        sub_node{ex, disc, make_id(0x0B), tb, make_opts(/*eager=*/true)};
    sub_node.listen({"inproc", "host-b:6000"});
    pub_node.listen({"inproc", "host-a:5000"});
    ex.drain();
    REQUIRE(sub_node.router().is_connected(pub_node.id()));

    std::vector<std::vector<std::byte>> got;
    bytes_subscriber s{sub_node, "topic",
                       [&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); }};
    counting_codec   codec;
    typed_publisher  p{pub_node, "topic", plexus::typed_publisher_options{}, codec};
    ex.drain();

    // The subscriber is bytes (undeclared type), so the typed publisher has no eligible
    // process-tier object subscriber: the byte path is taken and the codec encodes once.
    p.publish(sample{0xCAFEBABEu});
    ex.drain();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == encode_u32(0xCAFEBABEu));
    REQUIRE(codec.encodes->load() == 1);
}

TEST_CASE("typed interop cell 2: a bytes producer's valid encoding decodes to an equal T",
          "[node][typed][interop]")
{
    net n;
    n.connect();

    std::vector<sample> got;
    typed_subscriber    s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
    bytes_publisher     p{n.b, "topic"};
    n.drive();

    p.publish(as_bytes(encode_u32(0x0BADF00Du)));
    n.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front().value == 0x0BADF00Du);
}

TEST_CASE("typed interop cell 3: garbage from a bytes producer drops+counts by default, escapes on "
          "opt-in",
          "[node][typed][interop]")
{
    SECTION("default: dropped + counted, session stays up")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        typed_subscriber    s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
        bytes_publisher     p{n.b, "topic"};
        n.drive();

        p.publish(as_bytes(std::vector<std::byte>(7, std::byte{0xFF}))); // not 4 bytes
        n.drive();

        REQUIRE(got.empty());
        REQUIRE(s.decode_failed() == 1);
        REQUIRE(n.a.router().is_connected(n.id_b));
    }

    SECTION("opt-in escape: the raw bytes + errc surface")
    {
        net n;
        n.connect();

        std::vector<sample>              got;
        std::vector<std::size_t>         raw_sizes;
        std::vector<std::error_code>     ecs;
        plexus::typed_subscriber_options opts;
        opts.on_decode_failure = [&](std::span<const std::byte> raw, std::error_code ec)
        {
            raw_sizes.push_back(raw.size());
            ecs.push_back(ec);
        };
        typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
        bytes_publisher  p{n.b, "topic"};
        n.drive();

        p.publish(as_bytes(std::vector<std::byte>(9, std::byte{0xAB})));
        n.drive();

        REQUIRE(got.empty());
        REQUIRE(s.decode_failed() == 1);
        REQUIRE(raw_sizes.size() == 1);
        REQUIRE(raw_sizes.front() == 9);
        REQUIRE(ecs.front() == std::make_error_code(std::errc::invalid_argument));
    }
}

TEST_CASE("typed interop cell 4: a typed pair over a NON-process tier round-trips through "
          "encode+decode",
          "[node][typed][interop]")
{
    // Both endpoints are typed with the SAME identity, but the subscriber declares the
    // topic locality-confined to remote so the in-process object lane is never eligible —
    // forcing the encode->wire->decode round-trip even on one bus. The value equality after
    // a real serialize/deserialize is the cell-4 observable.
    net n;
    n.connect();

    std::vector<sample>              got;
    plexus::typed_subscriber_options sopts;
    sopts.qos.requested_reliability_reliable = false;
    typed_subscriber s{n.a, "topic", sopts, [&](const sample &v) { got.push_back(v); },
                       counting_codec{}};
    counting_codec   codec;
    auto             encodes = codec.encodes;
    // Force the byte path: publish(const T&) on an exhausted pool serializes directly. A
    // depth-0 pool has no slot, so every publish degrades to encode->wire->decode.
    plexus::typed_publisher_options popts;
    popts.pool_depth = 0;
    typed_publisher p{n.b, "topic", popts, codec};
    n.drive();

    p.publish(sample{0x13572468u});
    n.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front().value == 0x13572468u);
    REQUIRE(encodes->load() == 1);    // a real encode happened (no object fast path)
    REQUIRE(p.loan_exhausted() == 1); // the depth-0 pool degraded to serialize
}

TEST_CASE(
        "typed interop cell 5: a declared type mismatch is refused, no delivery, session stays up",
        "[node][typed][interop]")
{
    net n;
    n.connect();

    // The subscriber declares type Y (tag 0x2222...), the publisher type X (tag 0x1111...)
    // on the same topic: the producer refuses the subscribe with type_mismatch, registers
    // NO fan-out entry, and the session stays up. The behavioral observable is no delivery.
    counting_codec sub_codec;
    sub_codec.tag = 0x22222222u;
    counting_codec pub_codec;
    pub_codec.tag = 0x11111111u;

    std::vector<sample> got;
    typed_subscriber    s{n.a, "topic", plexus::typed_subscriber_options{},
                          [&](const sample &v) { got.push_back(v); }, sub_codec};
    typed_publisher     p{n.b, "topic", plexus::typed_publisher_options{}, pub_codec};
    n.drive();

    p.publish(sample{0x99u});
    n.drive();

    REQUIRE(got.empty());                       // the mismatch was refused at attach
    REQUIRE(n.a.router().is_connected(n.id_b)); // the session is not torn down
}

TEST_CASE(
        "typed interop cell 8: a typed inproc publisher to a bytes subscriber takes the byte path",
        "[node][typed][interop]")
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};
    inproc_node        pub_node{ex, disc, make_id(0x0A), ta, make_opts(/*eager=*/false)};
    inproc_node        sub_node{ex, disc, make_id(0x0B), tb, make_opts(/*eager=*/true)};
    sub_node.listen({"inproc", "host-b:6000"});
    pub_node.listen({"inproc", "host-a:5000"});
    ex.drain();
    REQUIRE(sub_node.router().is_connected(pub_node.id()));

    std::vector<std::vector<std::byte>> got;
    bytes_subscriber s{sub_node, "topic",
                       [&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); }};
    counting_codec   codec;
    auto             encodes = codec.encodes;
    // A borrowed loan would ride the fast path IF an eligible object subscriber existed; the
    // bytes subscriber has type_id nullopt so it is structurally ineligible. The publish
    // therefore serializes (encode invoked) and the bytes arrive as the encoding.
    typed_publisher p{pub_node, "topic", plexus::typed_publisher_options{}, codec};
    ex.drain();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0x44332211u;
    p.publish(std::move(loan));
    ex.drain();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == encode_u32(0x44332211u));
    REQUIRE(encodes->load() == 1); // the byte path was taken: exactly one encode
}

TEST_CASE("typed interop cell 9: a strict typed subscriber refuses an undeclared producer",
          "[node][typed][interop]")
{
    net n;
    n.connect();

    std::vector<sample>              got;
    plexus::typed_subscriber_options opts;
    opts.posture = plexus::io::attach_posture::strict;
    typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
    bytes_publisher  p{n.b, "topic"}; // never declares a type -> undeclared producer
    n.drive();

    p.publish(as_bytes(encode_u32(0x77u)));
    n.drive();

    REQUIRE(got.empty());                       // strict refused the undeclared producer
    REQUIRE(n.a.router().is_connected(n.id_b)); // refusal is per-topic, never a teardown
}

TEST_CASE("typed interop cell 10: a tag-equal carrier of a different C++ type is a counted drop, "
          "never a cast",
          "[node][typed][interop]")
{
    // Two distinct C++ types sharing ONE wire type_id in-process. The publisher's codec
    // encodes `other` under the same tag the subscriber's `sample` codec declares; the
    // process-tier object carrier therefore tag-matches but its native_key (a per-T
    // address witness) does NOT. The node demux counts the mismatch and warn-drops it —
    // never a reinterpret_cast, never the typed callback, never UB.
    struct other
    {
        std::uint64_t a{};
        std::uint64_t b{};
    };
    struct other_codec
    {
        using value_type                          = other;
        std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);
        plexus::wire_bytes<>              encode(const other &) const
        {
            ++*encodes;
            auto owner = std::make_shared<std::vector<std::byte>>(4, std::byte{0});
            return plexus::wire_bytes<>{{owner->data(), owner->size()}, std::move(owner)};
        }
        plexus::expected<void, std::error_code> decode(std::span<const std::byte>, other &) const
        {
            return {};
        }
        // The SAME tag the sample codec declares — a deliberate misconfiguration.
        plexus::type_identity type_info() const { return {0xABCD1234u, "other"}; }
    };
    static_assert(plexus::typed_codec<other_codec>);

    net n;
    n.connect();

    int                            sample_calls = 0;
    typed_subscriber               s{n.a, "topic", plexus::typed_subscriber_options{},
                                     [&](const sample &) { ++sample_calls; }, counting_codec{}};
    plexus::publisher<other_codec> p{n.b, "topic", plexus::typed_publisher_options{},
                                     other_codec{}};
    n.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->a = 0xDEADu;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(sample_calls == 0);                   // the typed callback never ran
    REQUIRE(n.a.object_dispatch_mismatch() == 1); // the demux counted the misconfig
    REQUIRE(n.a.router().is_connected(n.id_b));   // no crash, no teardown
}
