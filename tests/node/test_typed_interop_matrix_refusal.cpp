#include "test_typed_interop_matrix_common.h"

using namespace typed_interop_fixture;

TEST_CASE("typed interop cell 5: a declared type mismatch is refused, no delivery, session stays up", "[node][typed][interop]")
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
    typed_subscriber s{n.a, "topic", plexus::typed_subscriber_options{}, [&](const sample &v) { got.push_back(v); }, sub_codec};
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, pub_codec};
    n.drive();

    p.publish(sample{0x99u});
    n.drive();

    REQUIRE(got.empty());                       // the mismatch was refused at attach
    REQUIRE(n.a.router().is_connected(n.id_b)); // the session is not torn down
}

TEST_CASE("typed interop cell 8: a typed inproc publisher to a bytes subscriber takes the byte path", "[node][typed][interop]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};
    inproc_node pub_node{ex, disc, make_id(0x0A), ta, make_opts(/*eager=*/false)};
    inproc_node sub_node{ex, disc, make_id(0x0B), tb, make_opts(/*eager=*/true)};
    sub_node.listen({"inproc", "host-b:6000"});
    pub_node.listen({"inproc", "host-a:5000"});
    ex.drain();
    REQUIRE(sub_node.router().is_connected(pub_node.id()));

    std::vector<std::vector<std::byte>> got;
    bytes_subscriber s{sub_node, "topic", [&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); }};
    counting_codec codec;
    auto encodes = codec.encodes;
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

TEST_CASE("typed interop cell 9: a strict typed subscriber refuses an undeclared producer", "[node][typed][interop]")
{
    net n;
    n.connect();

    std::vector<sample> got;
    plexus::typed_subscriber_options opts;
    opts.posture = plexus::io::attach_posture::strict;
    typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
    bytes_publisher p{n.b, "topic"}; // never declares a type -> undeclared producer
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
        plexus::wire_bytes<> encode(const other &) const
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
        plexus::type_identity type_info() const
        {
            return {0xABCD1234u, "other"};
        }
    };
    static_assert(plexus::typed_codec<other_codec>);

    net n;
    n.connect();

    int sample_calls = 0;
    typed_subscriber s{n.a, "topic", plexus::typed_subscriber_options{}, [&](const sample &) { ++sample_calls; }, counting_codec{}};
    plexus::publisher<other_codec> p{n.b, "topic", plexus::typed_publisher_options{}, other_codec{}};
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
