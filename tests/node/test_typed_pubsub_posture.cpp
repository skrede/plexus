#include "test_typed_pubsub_common.h"

using namespace typed_pubsub_fixture;

TEST_CASE("typed pub/sub: a decode failure is dropped and counted by default, escaped on opt-in",
          "[node][typed][pubsub]")
{
    SECTION("default: drop + count, node stays connected")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        typed_subscriber    s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
        bytes_publisher     p{n.b, "topic"};
        n.drive();

        const std::vector<std::byte> garbage(7, std::byte{0xFF}); // not 4 bytes -> decode fails
        p.publish(as_bytes(garbage));
        n.drive();

        REQUIRE(got.empty());
        REQUIRE(s.decode_failed() == 1);
        REQUIRE(n.a.router().is_connected(n.id_b)); // never a teardown
    }

    SECTION("opt-in escape callback receives the raw bytes + errc")
    {
        net n;
        n.connect();

        std::vector<sample>              got;
        std::vector<std::size_t>         escaped_sizes;
        std::vector<std::error_code>     escaped_errcs;
        plexus::typed_subscriber_options opts;
        opts.on_decode_failure = [&](std::span<const std::byte> raw, std::error_code ec)
        {
            escaped_sizes.push_back(raw.size());
            escaped_errcs.push_back(ec);
        };
        typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
        bytes_publisher  p{n.b, "topic"};
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

TEST_CASE("typed pub/sub: strict posture refuses an undeclared bytes producer, lenient admits",
          "[node][typed][pubsub]")
{
    SECTION("strict: no delivery from an undeclared producer")
    {
        net n;
        n.connect();

        std::vector<sample>              got;
        plexus::typed_subscriber_options opts;
        opts.posture = plexus::io::attach_posture::strict;
        typed_subscriber s{n.a, "topic", opts, [&](const sample &v) { got.push_back(v); }};
        // A bytes publisher never declares a type_id -> undeclared producer.
        bytes_publisher p{n.b, "topic"};
        n.drive();

        const auto frame = encode_u32(0x11u);
        p.publish(as_bytes(frame));
        n.drive();

        REQUIRE(got.empty()); // strict refused the attach to an undeclared producer
    }

    SECTION("lenient default: admits an undeclared producer")
    {
        net n;
        n.connect();

        std::vector<sample> got;
        typed_subscriber    s{n.a, "topic", [&](const sample &v) { got.push_back(v); }};
        bytes_publisher     p{n.b, "topic"};
        n.drive();

        const auto frame = encode_u32(0x22u);
        p.publish(as_bytes(frame));
        n.drive();

        REQUIRE(got.size() == 1);
        REQUIRE(got.front().value == 0x22u);
    }
}

TEST_CASE("typed pub/sub: a dropped typed subscriber delivers nothing on a later publish",
          "[node][typed][pubsub]")
{
    net n;
    n.connect();

    int  delivered = 0;
    auto s = std::make_unique<typed_subscriber>(n.a, "topic", [&](const sample &) { ++delivered; });
    counting_codec  codec;
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
