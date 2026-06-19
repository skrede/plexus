// The self-describing capture oracle: a single-process inproc session whose producer
// declares a topic with a known producer type_id and attaches a recorder carrying a public
// type_schema, then captures, drains, and reads the flat stream back. It proves the public
// surface populates the version-2 preamble end to end: the four opaque schema fields survive
// byte-identically keyed by type_id, the per-capture crypto position equals the node's
// declared wire position, and a recovered sample carries the real producer type_id (not the
// hard-coded 0/false). A second case proves a recorder that declares nothing still writes a
// valid recoverable opaque stream, and a third proves a schema blob larger than the writer's
// default scratch round-trips (no preamble heap overrun).

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/wire_bytes.h"
#include "plexus/recorder.h"
#include "plexus/subscriber.h"
#include "plexus/type_schema.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recorder_options.h"

#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;

inline constexpr std::uint64_t k_reading_type_id = 0x9A9A0001u;

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

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes,
                                                   reading                   &out) const
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

    plexus::type_identity type_info() const { return {k_reading_type_id, "reading"}; }
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
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = true;
    return opts;
}

std::span<const std::byte> as_bytes(std::string_view s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Publish one reading through a payload-fidelity typed publisher and drain the recorder out.
template<typename Recorder>
void capture_one(bare_node &producer, bare_node &consumer, inproc_executor<> &ex,
                 Recorder &recorder, std::uint32_t value)
{
    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};

    plexus::typed_publisher_options pub_opts;
    pub_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    typed_publisher pub{producer, "telemetry", pub_opts, reading_codec{}};
    ex.drain();

    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = value;
        pub.publish(std::move(loan));
    }
    ex.drain();
    while(recorder.pump())
        ;
    recorder.flush();
}

}

TEST_CASE("a declared schema + crypto position + producer type_id round-trip through the preamble",
          "[self_describing_capture]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    plexus::node_options producer_opts = base_opts();
    producer_opts.wire.position        = plexus::wire_crypto_position::ciphertext;

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x0B), producer_tp, producer_opts};

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    const std::string schema_blob =
            R"({"type":"object","properties":{"value":{"type":"integer"}}})";

    plexus::recorder_options ropts;
    ropts.schemas.push_back(plexus::type_schema{.type_id          = k_reading_type_id,
                                                .message_encoding = "json",
                                                .schema_name      = "reading",
                                                .schema_encoding  = "jsonschema",
                                                .schema_data      = as_bytes(schema_blob)});

    in_memory_byte_sink sink;
    auto                recorder = producer.make_recorder(sink, std::move(ropts));

    const std::uint32_t published = 0xCAFEu;
    capture_one(producer, consumer, ex, recorder, published);

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions   defs;
    REQUIRE(reader.read_definitions(defs));

    // The preamble crypto position equals the node's declared wire position.
    REQUIRE(defs.crypto_position == plexus::io::recording::capture_crypto_position::ciphertext);

    // The declared schema survives byte-identically, keyed by type_id.
    REQUIRE(defs.schema.size() == 1);
    const auto &e = defs.schema.front();
    REQUIRE(e.type_id == k_reading_type_id);
    REQUIRE(e.message_encoding == "json");
    REQUIRE(e.schema_name == "reading");
    REQUIRE(e.schema_encoding == "jsonschema");
    const std::vector<std::byte> expected_blob{as_bytes(schema_blob).begin(),
                                               as_bytes(schema_blob).end()};
    REQUIRE(e.schema_data == expected_blob);

    std::vector<plexus::io::recording::decoded_record> records;
    const auto                                         recovery = reader.recover(records);
    REQUIRE(recovery.header_ok);

    const auto telemetry_hash = plexus::wire::fqn_topic_hash("telemetry");

    bool saw_typed_sample = false;
    for(const auto &rec : records)
    {
        if(rec.category != plexus::io::recording::record_category::sample)
            continue;
        if(rec.topic_hash != telemetry_hash || rec.payload.empty())
            continue;
        saw_typed_sample = true;
        // The sample carries the REAL producer type_id (not the old hard-coded 0/false).
        REQUIRE(rec.type_id.has_value());
        REQUIRE(*rec.type_id == k_reading_type_id);
    }
    REQUIRE(saw_typed_sample);
}

TEST_CASE("a recorder that declares nothing still writes a valid opaque stream",
          "[self_describing_capture]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x1A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x1B), producer_tp, base_opts()};

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    in_memory_byte_sink sink;
    auto                recorder = producer.make_recorder(sink); // default options: empty schemas

    capture_one(producer, consumer, ex, recorder, 0x1234u);

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions   defs;
    REQUIRE(reader.read_definitions(defs));
    REQUIRE(defs.schema.empty());
    // The unset node wire position defaults to cleartext.
    REQUIRE(defs.crypto_position == plexus::io::recording::capture_crypto_position::cleartext);

    std::vector<plexus::io::recording::decoded_record> records;
    REQUIRE(reader.recover(records).header_ok);
}

TEST_CASE("a declared schema larger than the writer's default scratch round-trips",
          "[self_describing_capture]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    static_discovery  disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x2A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x2B), producer_tp, base_opts()};

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    // A schema blob comfortably larger than the writer's 64 KiB default scratch — the
    // preamble must size the scratch up so begin_stream's raw-memcpy writer cannot overrun.
    const std::string big_blob(96u * 1024u, 'x');

    plexus::recorder_options ropts;
    ropts.schemas.push_back(plexus::type_schema{.type_id          = k_reading_type_id,
                                                .message_encoding = "json",
                                                .schema_name      = "reading",
                                                .schema_encoding  = "jsonschema",
                                                .schema_data      = as_bytes(big_blob)});

    in_memory_byte_sink sink;
    auto                recorder = producer.make_recorder(sink, std::move(ropts));

    capture_one(producer, consumer, ex, recorder, 0xBEEFu);

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions   defs;
    REQUIRE(reader.read_definitions(defs));
    REQUIRE(defs.schema.size() == 1);
    REQUIRE(defs.schema.front().schema_data.size() == big_blob.size());
    const std::vector<std::byte> expected_blob{as_bytes(big_blob).begin(),
                                               as_bytes(big_blob).end()};
    REQUIRE(defs.schema.front().schema_data == expected_blob);
}
