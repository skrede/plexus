#include "test_self_describing_capture_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace self_describing_fixture;

TEST_CASE("a declared schema larger than the writer's default scratch round-trips", "[self_describing_capture]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

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
    ropts.schemas.push_back(
            plexus::type_schema{.type_id = k_reading_type_id, .message_encoding = "json", .schema_name = "reading", .schema_encoding = "jsonschema", .schema_data = as_bytes(big_blob)});

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink, std::move(ropts));

    capture_one(producer, consumer, ex, recorder, 0xBEEFu);

    const auto stream = sink.bytes();
    REQUIRE(!stream.empty());

    plexus::io::recording::record_stream_reader reader{stream};
    plexus::io::recording::stream_definitions defs;
    REQUIRE(reader.read_definitions(defs));
    REQUIRE(defs.schema.size() == 1);
    REQUIRE(defs.schema.front().schema_data.size() == big_blob.size());
    const std::vector<std::byte> expected_blob{as_bytes(big_blob).begin(), as_bytes(big_blob).end()};
    REQUIRE(defs.schema.front().schema_data == expected_blob);
}
