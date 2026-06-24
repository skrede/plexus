// The payload-fidelity capture oracle: a single-process inproc session whose producer
// declares a payload-fidelity capture rule on a topic, publishes a known value through the
// public typed publisher, drains the recorder into an in-memory flat stream, and reads the
// stream back with record_stream_reader. It asserts the recorded sample carries the BARE
// codec bytes (the 4-byte u32 the codec writes — NOT the framed buffer with its 45+ byte
// frame prefix), byte-identical to encode(), and stamped with the resolved payload fidelity.
// This is the payload-channel sibling of the wire-channel transcode oracle.

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"

#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <chrono>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;

struct reading
{
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, reading &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x9A9A0001u, "reading"};
    }
};

static_assert(plexus::typed_codec<reading_codec>);

using typed_publisher  = plexus::publisher<reading_codec>;
using typed_subscriber = plexus::subscriber<reading_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options base_opts()
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = true;
    return opts;
}

}

TEST_CASE("a payload-fidelity sample records the bare codec bytes, not the framed buffer", "[payload_fidelity_capture]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x0B), producer_tp, base_opts()};

    in_memory_byte_sink sink;
    auto                recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};

    plexus::typed_publisher_options pub_opts;
    pub_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    typed_publisher pub{producer, "telemetry", pub_opts, reading_codec{}};
    ex.drain();

    const std::uint32_t published = 0xCAFEu;
    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = published;
        pub.publish(std::move(loan));
    }
    ex.drain();
    while(recorder.pump())
        ;
    recorder.flush();

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions   defs;
    REQUIRE(reader.read_definitions(defs));

    std::vector<plexus::io::recording::decoded_record> records;
    const auto                                         recovery = reader.recover(records);
    REQUIRE(recovery.header_ok);

    const auto telemetry_hash = plexus::wire::fqn_topic_hash("telemetry");

    bool saw_payload_sample = false;
    for(const auto &rec : records)
    {
        if(rec.category != plexus::io::recording::record_category::sample)
            continue;
        if(rec.topic_hash != telemetry_hash)
            continue;
        if(rec.fidelity != plexus::io::capture_fidelity::payload)
            continue;
        if(rec.payload.empty())
            continue;
        saw_payload_sample = true;

        // The bare codec output is exactly 4 bytes — the frame prefix is fully stripped.
        REQUIRE(rec.payload.size() == 4);

        std::uint32_t recovered = 0;
        for(int i = 0; i < 4; ++i)
            recovered |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(rec.payload[i])) << (8 * i);
        REQUIRE(recovered == published);
    }
    REQUIRE(saw_payload_sample);
}
