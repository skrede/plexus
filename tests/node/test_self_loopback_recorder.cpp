// Recorder/capture taps observe self-delivery. The bytes lane fires emit_published/emit_delivered on
// the self subscriber the same as any subscriber, so a public-API recorder over a byte_sink captures
// the self message: the recovered stream carries a sample whose framed tail is the published payload.
// The object lane records the EVENT always and the framed payload only when capture wants it — the
// existing behavior, identical to the SHM object lane (verified, not "fixed").

#include "plexus/recorder.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/message_info.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/wire/topic_hash.h"

#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <algorithm>

using plexus::io::message_info;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;
using plexus_test::sample;
using plexus_test::fixture;
using plexus_test::counting_codec;

namespace {

class memory_sink final : public plexus::io::recording::byte_sink
{
public:
    void write(std::span<const std::byte> bytes) override
    {
        m_bytes.insert(m_bytes.end(), bytes.begin(), bytes.end());
    }

    std::span<const std::byte> bytes() const noexcept
    {
        return m_bytes;
    }

private:
    std::vector<std::byte> m_bytes;
};

bool payload_ends_with(std::span<const std::byte> framed, std::span<const std::byte> marker)
{
    if(framed.size() < marker.size())
        return false;
    return std::equal(marker.begin(), marker.end(), framed.end() - marker.size());
}

}

TEST_CASE("self_loopback_recorder: the bytes lane self-delivery is captured", "[node][loopback][recorder]")
{
    fixture f;
    memory_sink sink;

    {
        plexus::recorder_options ro;
        ro.ring_bytes = 1u << 20;
        auto rec      = f.node().make_recorder(sink, std::move(ro));

        std::vector<std::vector<std::byte>> seen;
        plexus::subscriber<> s{f.node(), "topic", [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
        plexus::publisher<> p{f.node(), "topic"};
        f.drive();

        const std::array<std::byte, 4> mk{std::byte{0xA0}, std::byte{0x01}, std::byte{0xBE}, std::byte{0xEF}};
        p.publish(mk);
        f.drive(); // the published tap posts; the cooperative drain ships to the sink

        REQUIRE(seen.size() == 1); // self-delivery happened (the tap rides the same path)
        f.drive();
        rec.flush();
    }
    f.drive(); // drain post-recorder-dtor: no posted closure touches a freed ring (asan is the gate)

    record_stream_reader reader{sink.bytes()};
    stream_definitions defs;
    REQUIRE(reader.read_definitions(defs));

    std::vector<decoded_record> records;
    const recovery_result res = reader.recover(records);
    REQUIRE(res.header_ok);

    std::size_t samples = 0;
    bool saw_topic      = false;
    for(const auto &r : records)
    {
        if(r.category != record_category::sample)
            continue;
        ++samples;
        if(r.topic_hash == plexus::wire::fqn_topic_hash("topic"))
            saw_topic = true;
        const std::array<std::byte, 2> tail{std::byte{0xBE}, std::byte{0xEF}};
        REQUIRE(payload_ends_with(r.payload, tail));
    }
    REQUIRE(saw_topic);
    // Two sample records for the one self-publish: the once-per-publish published edge and the
    // once-per-destination delivered edge — both taps fired on the self subscriber, both carry the
    // framed payload (the witness self-delivery is observed exactly like any subscriber).
    REQUIRE(samples == 2);
}

TEST_CASE("self_loopback_recorder: the typed lane self-delivery is recorded as an event", "[node][loopback][recorder]")
{
    fixture f;
    memory_sink sink;

    {
        plexus::recorder_options ro;
        ro.ring_bytes = 1u << 20;
        auto rec      = f.node().make_recorder(sink, std::move(ro));

        std::vector<std::uint32_t> seen;
        plexus::subscriber<counting_codec> s{f.node(), "topic", [&](const sample &v) { seen.push_back(v.value); }};
        counting_codec codec;
        plexus::publisher<counting_codec> p{f.node(), "topic", plexus::typed_publisher_options{}, codec};
        f.drive();

        auto loan = p.borrow();
        REQUIRE(loan);
        loan->value = 0xABCDu;
        p.publish(std::move(loan));
        f.drive();

        REQUIRE(seen.size() == 1);
        REQUIRE(seen.front() == 0xABCDu);
        f.drive();
        rec.flush();
    }
    f.drive();

    // The object lane records the EVENT (a sample record for the self-delivered topic). Without
    // payload capture configured, the framed payload may be empty — the event is the proof the tap
    // observed the self object-delivery (identical to the SHM object lane).
    record_stream_reader reader{sink.bytes()};
    stream_definitions defs;
    REQUIRE(reader.read_definitions(defs));

    std::vector<decoded_record> records;
    const recovery_result res = reader.recover(records);
    REQUIRE(res.header_ok);

    bool saw_topic = false;
    for(const auto &r : records)
        if(r.category == record_category::sample && r.topic_hash == plexus::wire::fqn_topic_hash("topic"))
            saw_topic = true;
    REQUIRE(saw_topic);
}
