#include "test_publish_object_common.h"

using namespace publish_object_fixture;

TEST_CASE("publish_object: a process-excluding reach mask produces no object and no encode", "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    plexus::log::null_logger sink;
    forwarder fwd{sink};
    sink_peer s(ex, "node-a");

    // reach excludes the process tier, so the single same-process subscriber is gated
    // out before eligibility — no fast path, and (no byte-eligible subscriber remains)
    // the encode fn is never invoked.
    plexus::topic_qos qos;
    qos.reach = plexus::io::locality::remote;
    fwd.declare("alpha", qos, k_tag);
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", k_tag));

    counted_payload p;
    p.value          = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
                       });
    ex.drain();

    CHECK(s.objects.empty());
    CHECK(count_data_frames(s) == 0);
    CHECK(encode_calls == 0);
    CHECK(p.release_calls == 1);
}

TEST_CASE("publish_object: a latched topic forces one encode even when every subscriber fast-paths", "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    plexus::log::null_logger sink;
    forwarder fwd{sink};
    sink_peer typed(ex, "node-typed");

    plexus::topic_qos qos;
    qos.latch = true;
    qos.depth = 1;
    fwd.declare("alpha", qos, k_tag);
    REQUIRE(fwd.attach_for_fanout(typed.peer(), "alpha", k_tag));

    counted_payload p;
    p.value          = "latched-payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
                       });
    ex.drain();

    CHECK(typed.objects.size() == 1); // the live subscriber still fast-pathed
    CHECK(encode_calls == 1);         // but the latch forced exactly one encode
    CHECK(p.release_calls == 1);

    // A late BYTES joiner (durability=latest) replays the encoded frame the latch
    // retained — proving the all-fast-path publish still left real bytes in history.
    plexus::io::subscriber_qos latest_qos;
    latest_qos.durability_mode = plexus::io::durability::latest;
    sink_peer late(ex, "node-late");
    REQUIRE(fwd.attach_for_fanout(late.peer(), "alpha", std::nullopt, latest_qos));
    ex.drain();
    CHECK(count_data_frames(late) == 1);
}

TEST_CASE("publish_object: the caller reference is balanced when no subscriber matches", "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    plexus::log::null_logger sink;
    forwarder fwd{sink};

    // No subscribers, unlatched topic: nothing to deliver, encode never runs, but the
    // caller's reference must still be released exactly once.
    fwd.declare("alpha", plexus::topic_qos{}, k_tag);

    counted_payload p;
    p.value          = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag),
                       [&]
                       {
                           ++encode_calls;
                           return std::span<const std::byte>{reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
                       });
    ex.drain();

    CHECK(encode_calls == 0);
    CHECK(p.slot.refs == 0u);
    CHECK(p.release_calls == 1);
}
