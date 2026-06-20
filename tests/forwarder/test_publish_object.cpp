#include "test_publish_object_common.h"

using namespace publish_object_fixture;

TEST_CASE(
        "publish_object: a matching process-tier subscriber receives the object, encode never runs",
        "[forwarder][object]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    forwarder         fwd{};
    sink_peer         s(ex, "node-a");

    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", k_tag));

    counted_payload p;
    p.value          = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{
                                   reinterpret_cast<const std::byte *>(p.value.data()),
                                   p.value.size()};
                       });
    ex.drain();

    REQUIRE(s.objects.size() == 1);
    CHECK(s.objects[0].type_tag == k_tag);
    CHECK(s.objects[0].slot == &p.slot);
    CHECK(encode_calls == 0);
    CHECK(count_data_frames(s) == 0);
    CHECK(p.slot.refs == 0u);
    CHECK(p.release_calls == 1);
}

TEST_CASE("publish_object: a bytes-family subscriber takes the byte path with one encode",
          "[forwarder][object]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    forwarder         fwd{};
    sink_peer         s(ex, "node-a");

    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    // No subscriber type_id: a bytes-family attach. Eligibility's type_id gate fails.
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", std::nullopt));

    counted_payload p;
    p.value          = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{
                                   reinterpret_cast<const std::byte *>(p.value.data()),
                                   p.value.size()};
                       });
    ex.drain();

    CHECK(s.objects.empty());
    CHECK(encode_calls == 1);
    CHECK(count_data_frames(s) == 1);
    CHECK(p.release_calls == 1);
}

TEST_CASE(
        "publish_object: a mixed subscriber set delivers object AND bytes with exactly one encode",
        "[forwarder][object]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    forwarder         fwd{};
    sink_peer         typed(ex, "node-typed");
    sink_peer         bytes(ex, "node-bytes");

    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    REQUIRE(fwd.attach_for_fanout(typed.peer(), "alpha", k_tag));
    REQUIRE(fwd.attach_for_fanout(bytes.peer(), "alpha", std::nullopt));

    counted_payload p;
    p.value          = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{
                                   reinterpret_cast<const std::byte *>(p.value.data()),
                                   p.value.size()};
                       });
    ex.drain();

    CHECK(typed.objects.size() == 1);
    CHECK(count_data_frames(typed) == 0);
    CHECK(bytes.objects.empty());
    CHECK(count_data_frames(bytes) == 1);
    CHECK(encode_calls == 1);
    CHECK(p.release_calls == 1);
}

TEST_CASE("publish_object: a tag mismatch falls back to the byte path", "[forwarder][object]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    forwarder         fwd{};
    sink_peer         s(ex, "node-a");

    // Producer + subscriber both declare the SAME type (so attach is accepted), but
    // the published carrier carries a DIFFERENT wire tag — eligibility's tag compare
    // fails and the subscriber takes bytes.
    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", k_tag));

    counted_payload p;
    p.value          = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, 0x9999),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{
                                   reinterpret_cast<const std::byte *>(p.value.data()),
                                   p.value.size()};
                       });
    ex.drain();

    CHECK(s.objects.empty());
    CHECK(encode_calls == 1);
    CHECK(count_data_frames(s) == 1);
    CHECK(p.release_calls == 1);
}
