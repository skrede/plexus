// publish_object: the zero-serialization sibling of publish. A same-process
// subscriber whose stored type_id matches the carrier's wire tag receives the object
// handle through the inproc lane with the encode callback never invoked; every
// ineligible subscriber (bytes family, tag mismatch, no object lane) takes the byte
// path with encode invoked AT MOST ONCE per publish; a latched topic forces exactly
// one encode even when every live subscriber fast-paths; the caller's slot reference
// is balanced on every path.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/object_carrier.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::io::loan_slot;
using plexus::io::object_carrier;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace {

struct counted_payload
{
    std::string value;
    int release_calls{0};
    loan_slot slot{};
};

object_carrier make_carrier(counted_payload &p, std::uint64_t tag)
{
    p.slot.object = &p.value;
    p.slot.refs = 1;   // the caller owns one reference on entry to publish_object
    p.slot.release = [](loan_slot *s) {
        auto *owner = reinterpret_cast<counted_payload *>(
            reinterpret_cast<std::byte *>(s) - offsetof(counted_payload, slot));
        ++owner->release_calls;
    };
    return object_carrier{0, tag, &p.value, 0, 0, &p.slot};
}

// A capture sink recording both the byte frames and the object carriers the forwarder
// fans toward a peer's channel.
struct sink_peer
{
    explicit sink_peer(inproc_executor<> &ex, std::string node_name)
        : fwd_channel(ex), sink(ex), name(std::move(node_name))
    {
        fwd_channel.connect_to(sink.local_endpoint());
        sink.on_data([this](std::span<const std::byte> d) { byte_frames.emplace_back(d.begin(), d.end()); });
        sink.on_object([this](const object_carrier &c) {
            objects.push_back(c);
            plexus::io::release(c);   // the receiving handler owns the delivered reference
        });
    }

    forwarder::peer peer() { return forwarder::peer{fwd_channel, name}; }

    inproc_channel<> fwd_channel;
    inproc_channel<> sink;
    std::string name;
    std::vector<std::vector<std::byte>> byte_frames;
    std::vector<object_carrier> objects;
};

std::size_t count_data_frames(const sink_peer &s)
{
    std::size_t n = 0;
    for(const auto &f : s.byte_frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(hdr && hdr->type == plexus::wire::msg_type::unidirectional)
            ++n;
    }
    return n;
}

constexpr std::uint64_t k_tag = 0x7777;

}

TEST_CASE("publish_object: a matching process-tier subscriber receives the object, encode never runs",
          "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};
    sink_peer s(ex, "node-a");

    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", k_tag));

    counted_payload p;
    p.value = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
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
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};
    sink_peer s(ex, "node-a");

    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    // No subscriber type_id: a bytes-family attach. Eligibility's type_id gate fails.
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", std::nullopt));

    counted_payload p;
    p.value = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
    });
    ex.drain();

    CHECK(s.objects.empty());
    CHECK(encode_calls == 1);
    CHECK(count_data_frames(s) == 1);
    CHECK(p.release_calls == 1);
}

TEST_CASE("publish_object: a mixed subscriber set delivers object AND bytes with exactly one encode",
          "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};
    sink_peer typed(ex, "node-typed");
    sink_peer bytes(ex, "node-bytes");

    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    REQUIRE(fwd.attach_for_fanout(typed.peer(), "alpha", k_tag));
    REQUIRE(fwd.attach_for_fanout(bytes.peer(), "alpha", std::nullopt));

    counted_payload p;
    p.value = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
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
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};
    sink_peer s(ex, "node-a");

    // Producer + subscriber both declare the SAME type (so attach is accepted), but
    // the published carrier carries a DIFFERENT wire tag — eligibility's tag compare
    // fails and the subscriber takes bytes.
    fwd.declare("alpha", plexus::topic_qos{}, k_tag);
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", k_tag));

    counted_payload p;
    p.value = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, 0x9999), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
    });
    ex.drain();

    CHECK(s.objects.empty());
    CHECK(encode_calls == 1);
    CHECK(count_data_frames(s) == 1);
    CHECK(p.release_calls == 1);
}

TEST_CASE("publish_object: a process-excluding reach mask produces no object and no encode",
          "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};
    sink_peer s(ex, "node-a");

    // reach excludes the process tier, so the single same-process subscriber is gated
    // out before eligibility — no fast path, and (no byte-eligible subscriber remains)
    // the encode fn is never invoked.
    plexus::topic_qos qos;
    qos.reach = plexus::io::locality::remote;
    fwd.declare("alpha", qos, k_tag);
    REQUIRE(fwd.attach_for_fanout(s.peer(), "alpha", k_tag));

    counted_payload p;
    p.value = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
    });
    ex.drain();

    CHECK(s.objects.empty());
    CHECK(count_data_frames(s) == 0);
    CHECK(encode_calls == 0);
    CHECK(p.release_calls == 1);
}

TEST_CASE("publish_object: a latched topic forces one encode even when every subscriber fast-paths",
          "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};
    sink_peer typed(ex, "node-typed");

    plexus::topic_qos qos;
    qos.latch = true;
    qos.depth = 1;
    fwd.declare("alpha", qos, k_tag);
    REQUIRE(fwd.attach_for_fanout(typed.peer(), "alpha", k_tag));

    counted_payload p;
    p.value = "latched-payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
    });
    ex.drain();

    CHECK(typed.objects.size() == 1);   // the live subscriber still fast-pathed
    CHECK(encode_calls == 1);           // but the latch forced exactly one encode
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

TEST_CASE("publish_object: the caller reference is balanced when no subscriber matches",
          "[forwarder][object]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    forwarder fwd{ex};

    // No subscribers, unlatched topic: nothing to deliver, encode never runs, but the
    // caller's reference must still be released exactly once.
    fwd.declare("alpha", plexus::topic_qos{}, k_tag);

    counted_payload p;
    p.value = "payload";
    int encode_calls = 0;
    fwd.publish_object("alpha", make_carrier(p, k_tag), [&] {
        ++encode_calls;
        return std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(p.value.data()), p.value.size()};
    });
    ex.drain();

    CHECK(encode_calls == 0);
    CHECK(p.slot.refs == 0u);
    CHECK(p.release_calls == 1);
}
