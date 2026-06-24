#include "test_typed_interop_matrix_common.h"

using namespace typed_interop_fixture;

TEST_CASE("typed interop cell 1: a typed publisher reaches a bytes subscriber with the codec's "
          "encoding",
          "[node][typed][interop]")
{
    // A is the publisher node (eager so its demand-less role still dials), B the bytes
    // subscriber — flip the eager flag so the bytes subscriber's node is the single dialer.
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
    typed_publisher p{pub_node, "topic", plexus::typed_publisher_options{}, codec};
    ex.drain();

    // The subscriber is bytes (undeclared type), so the typed publisher has no eligible
    // process-tier object subscriber: the byte path is taken and the codec encodes once.
    p.publish(sample{0xCAFEBABEu});
    ex.drain();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == encode_u32(0xCAFEBABEu));
    REQUIRE(codec.encodes->load() == 1);
}

TEST_CASE("typed interop cell 2: a bytes producer's valid encoding decodes to an equal T", "[node][typed][interop]")
{
    net n;
    n.connect();

    std::vector<sample> got;
    typed_subscriber s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
    bytes_publisher p{n.b, "topic"};
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
        typed_subscriber s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
        bytes_publisher p{n.b, "topic"};
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

        std::vector<sample> got;
        std::vector<std::size_t> raw_sizes;
        std::vector<std::error_code> ecs;
        plexus::typed_subscriber_options opts;
        opts.on_decode_failure = [&](std::span<const std::byte> raw, std::error_code ec)
        {
            raw_sizes.push_back(raw.size());
            ecs.push_back(ec);
        };
        typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
        bytes_publisher p{n.b, "topic"};
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

    std::vector<sample> got;
    plexus::typed_subscriber_options sopts;
    sopts.qos.requested_reliability_reliable = false;
    typed_subscriber s{n.a, "topic", sopts, [&](const sample &v) { got.push_back(v); }, counting_codec{}};
    counting_codec codec;
    auto encodes = codec.encodes;
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
