#include "test_self_describing_capture_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace self_describing_fixture;

TEST_CASE("a declared schema + crypto position + producer type_id round-trip through the preamble", "[self_describing_capture]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    plexus::node_options producer_opts = base_opts();
    producer_opts.wire.position        = plexus::wire_crypto_position::ciphertext;

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x0B), producer_tp, producer_opts};

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    const std::string schema_blob = R"({"type":"object","properties":{"value":{"type":"integer"}}})";

    plexus::recorder_options ropts;
    ropts.schemas.push_back(plexus::type_schema{
            .type_id = k_reading_type_id, .message_encoding = "json", .schema_name = "reading", .schema_encoding = "jsonschema", .schema_data = as_bytes(schema_blob)});

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink, std::move(ropts));

    const std::uint32_t published = 0xCAFEu;
    capture_one(producer, consumer, ex, recorder, published);

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions defs;
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
    const std::vector<std::byte> expected_blob{as_bytes(schema_blob).begin(), as_bytes(schema_blob).end()};
    REQUIRE(e.schema_data == expected_blob);

    std::vector<plexus::io::recording::decoded_record> records;
    const auto recovery = reader.recover(records);
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

TEST_CASE("a recorder that declares nothing still writes a valid opaque stream", "[self_describing_capture]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    bare_node consumer{ex, disc, make_id(0x1A), consumer_tp, base_opts()};
    bare_node producer{ex, disc, make_id(0x1B), producer_tp, base_opts()};

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
    ex.drain();

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink); // default options: empty schemas

    capture_one(producer, consumer, ex, recorder, 0x1234u);

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions defs;
    REQUIRE(reader.read_definitions(defs));
    REQUIRE(defs.schema.empty());
    // The unset node wire position defaults to cleartext.
    REQUIRE(defs.crypto_position == plexus::io::recording::capture_crypto_position::cleartext);

    std::vector<plexus::io::recording::decoded_record> records;
    REQUIRE(reader.recover(records).header_ok);
}
