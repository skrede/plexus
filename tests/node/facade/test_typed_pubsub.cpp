#include "test_typed_pubsub_common.h"

using namespace typed_pubsub_fixture;

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
    typed_subscriber s{n.a, "topic", [&](const sample &v, const message_info &info)
                       {
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
    loan->value                  = 0x1234u;
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

TEST_CASE("typed pub/sub: publish(const T&) delivers value-equal over the fast path with zero "
          "encodes",
          "[node][typed][pubsub]")
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
