#include "test_record_stream_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace record_stream_fixture;

TEST_CASE("recording_sink captures synthetic edges through the recorder to a byte_sink", "[record_stream][recorder]")
{
    in_memory_byte_sink sink;
    std::uint64_t       tick = 0;
    flat_recorder       recorder{sink, 64u * 1024u, [&tick] { return ++tick; }};

    recorder.open(make_node(2), topic_capture_rule{});

    recording_sink tap{recorder};
    REQUIRE(tap.observes_data_path());

    plexus::io::participant_event pe{};
    pe.edge = plexus::io::participant_edge::created;
    pe.self = make_node(2);
    tap.on_participant(pe);

    endpoint_event ee{};
    ee.edge       = endpoint_edge::publisher_declared;
    ee.topic_hash = plexus::wire::fqn_topic_hash("sensor/imu");
    tap.on_endpoint("sensor/imu", ee);

    const std::string              body = "imu-sample-bytes";
    const plexus::io::message_view view{bytes_of(body), {}};
    message_info                   info{};
    info.publication_sequence = 9;
    tap.on_message_delivered("sensor/imu", info, view);

    while(recorder.pump())
        ;
    recorder.flush();

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    REQUIRE(defs.node == make_node(2));

    std::vector<decoded_record> out;
    const recovery_result       res = r.recover(out);
    REQUIRE(res.header_ok);
    REQUIRE_FALSE(res.corruption_skipped);
    REQUIRE(out.size() == 3);

    REQUIRE(out[0].category == record_category::participant);
    REQUIRE(out[1].category == record_category::endpoint);
    REQUIRE(out[1].fqn == "sensor/imu");

    REQUIRE(out[2].category == record_category::sample);
    REQUIRE(out[2].topic_hash == plexus::wire::fqn_topic_hash("sensor/imu"));
    REQUIRE(out[2].publication_sequence == 9);
    const std::string got{reinterpret_cast<const char *>(out[2].payload.data()), out[2].payload.size()};
    REQUIRE(got == body);
}

TEST_CASE("record stream round-trips the opaque schema table and crypto position offline", "[record_stream]")
{
    record_stream_writer w;
    in_memory_byte_sink  sink;

    const std::array<std::byte, 4> blob_a{std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}};
    const std::array<std::byte, 2> blob_b{std::byte{0xFE}, std::byte{0xED}};

    const std::array<type_schema_entry, 2> rows{type_schema_entry{0xC0DEu, "vendor.Imu", "enc/a", "Imu.schema", "schema/a", blob_a},
                                                type_schema_entry{0xBEEFu, "vendor.Gps", "enc/b", "Gps.schema", "schema/b", blob_b}};

    sink.write(w.begin_stream(99u, make_node(7), topic_capture_rule{}, std::span<const type_schema_entry>{rows}, capture_crypto_position::ciphertext));

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    REQUIRE(defs.clock_epoch == 99u);
    REQUIRE(defs.node == make_node(7));
    REQUIRE(defs.crypto_position == capture_crypto_position::ciphertext);
    REQUIRE(defs.schema.size() == 2);

    for(std::size_t i = 0; i < rows.size(); ++i)
    {
        const schema_definition &got = defs.schema[i];
        const type_schema_entry &src = rows[i];
        REQUIRE(got.type_id == src.type_id);
        REQUIRE(got.type_name == src.type_name);
        REQUIRE(got.message_encoding == src.message_encoding);
        REQUIRE(got.schema_name == src.schema_name);
        REQUIRE(got.schema_encoding == src.schema_encoding);
        REQUIRE(got.schema_data == std::vector<std::byte>(src.schema_data.begin(), src.schema_data.end()));
    }
}

TEST_CASE("record stream recovers an empty schema table with a default crypto position", "[record_stream]")
{
    record_stream_writer w;
    in_memory_byte_sink  sink;

    sink.write(w.begin_stream(3u, make_node(8), topic_capture_rule{}, {}));

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    REQUIRE(defs.clock_epoch == 3u);
    REQUIRE(defs.schema.empty());
    REQUIRE(defs.crypto_position == capture_crypto_position::cleartext);
}

TEST_CASE("record stream rejects a preamble written at the prior format version", "[record_stream]")
{
    record_stream_writer w;
    in_memory_byte_sink  sink;
    sink.write(w.begin_stream(1u, make_node(1), topic_capture_rule{}, {}));

    // Rewrite the version field (the u16 right after the u32 magic) to the prior version; the
    // reader must reject it — there is no cross-version compatibility shim.
    std::vector<std::byte> stream{sink.bytes().begin(), sink.bytes().end()};
    stream[sizeof(std::uint32_t)]     = std::byte{0x00};
    stream[sizeof(std::uint32_t) + 1] = std::byte{0x01};

    record_stream_reader r{stream};
    stream_definitions   defs;
    REQUIRE_FALSE(r.read_definitions(defs));
}
