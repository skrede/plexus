#include "test_record_stream_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace record_stream_fixture;

TEST_CASE("record_wire over a saturated ring sheds at the wire tier", "[record_stream][wire][recorder]")
{
    in_memory_byte_sink sink;
    std::uint64_t       tick = 0;
    // A tiny ring so a couple of wire frames overflow it before a drain runs.
    flat_recorder recorder{sink, 256u, [&tick] { return ++tick; }};
    recorder.open(make_node(5), topic_capture_rule{});

    const node_id          peer = make_node(9);
    std::vector<std::byte> big(200, std::byte{0x5A});

    // Fill then overflow: the ~240-byte framed records exceed the 256-byte ring one at a
    // time, so every admit past the first shed into the dropout run (drop-newest).
    for(int i = 0; i < 8; ++i)
        recorder.record_wire(wire_direction::out, static_cast<std::uint64_t>(i), peer, std::span<const std::byte>{big});

    // Drain to free the ring, then a small admit surfaces the accumulated dropout_record
    // BEFORE it (the gap is quantified on the first successful admit after a shed run).
    while(recorder.pump())
        ;
    std::vector<std::byte> small{std::byte{0x01}};
    recorder.record_wire(wire_direction::in, 99u, peer, std::span<const std::byte>{small});

    while(recorder.pump())
        ;
    recorder.flush();

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    REQUIRE(r.recover(out).header_ok);

    bool saw_dropout = false;
    for(const auto &rec : out)
        if(rec.category == record_category::dropout)
        {
            saw_dropout = true;
            REQUIRE(rec.fidelity == capture_fidelity::wire);
        }
    REQUIRE(saw_dropout);
}

TEST_CASE("record stream round-trips mixed categories offline", "[record_stream]")
{
    record_stream_writer w;
    byte_ring            ring{64u * 1024u};
    in_memory_byte_sink  sink;
    const std::string    payload = "the-quick-brown-fox-payload-bytes";

    const auto stream = encode_mixed_stream(w, ring, sink, payload);

    record_stream_reader r{stream};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    REQUIRE(defs.clock_epoch == 7u);
    REQUIRE(defs.node == make_node(1));

    std::vector<decoded_record> out;
    const recovery_result       res = r.recover(out);
    REQUIRE(res.header_ok);
    REQUIRE_FALSE(res.trailing_partial_dropped);
    REQUIRE_FALSE(res.corruption_skipped);
    REQUIRE(out.size() == 4);

    REQUIRE(out[0].category == record_category::sample);
    REQUIRE(out[0].topic_hash == plexus::wire::fqn_topic_hash("topic/a"));
    REQUIRE(out[0].publication_sequence == 42);
    REQUIRE(out[0].source_timestamp == 100);
    REQUIRE(out[0].type_id.has_value());
    REQUIRE(*out[0].type_id == 0xC0DEu);
    REQUIRE(out[0].fidelity == capture_fidelity::payload);

    // No codec in the stream: the raw payload bytes are byte-identical; a codec is applied
    // only here in the test to interpret them.
    const std::string decoded{reinterpret_cast<const char *>(out[0].payload.data()), out[0].payload.size()};
    REQUIRE(decoded == payload);

    REQUIRE(out[1].category == record_category::sample);
    REQUIRE(out[1].fidelity == capture_fidelity::metadata);
    REQUIRE(out[1].payload.empty());
    REQUIRE_FALSE(out[1].type_id.has_value());

    REQUIRE(out[2].category == record_category::drop);
    REQUIRE(out[2].count == 5);

    REQUIRE(out[3].category == record_category::endpoint);
    REQUIRE(out[3].fqn == "topic/a");
    REQUIRE(out[3].type_id.has_value());
}

TEST_CASE("record stream recovers every complete record after mid-write truncation", "[record_stream]")
{
    record_stream_writer w;
    byte_ring            ring{64u * 1024u};
    in_memory_byte_sink  sink;
    const std::string    payload = "truncation-test-payload";

    const auto full = encode_mixed_stream(w, ring, sink, payload);

    record_stream_reader whole{full};
    stream_definitions   wd;
    REQUIRE(whole.read_definitions(wd));
    std::vector<decoded_record> whole_out;
    const std::size_t           complete = whole.recover(whole_out).recovered;
    REQUIRE(complete == 4);

    // Truncate at every offset from just past the header into the tail; each truncation
    // recovers every record whose last byte survived and loses only the trailing partial.
    for(std::size_t cut = full.size() - 1; cut > full.size() / 2; --cut)
    {
        const std::span<const std::byte> truncated{full.data(), cut};
        record_stream_reader             rt{truncated};
        stream_definitions               td;
        REQUIRE(rt.read_definitions(td));

        std::vector<decoded_record> recovered;
        const recovery_result       res = rt.recover(recovered);

        REQUIRE(res.recovered <= complete);
        // Whatever was recovered is a PREFIX of the whole decode (no record reordered or
        // invented), and the count never exceeds the complete count.
        for(std::size_t i = 0; i < res.recovered; ++i)
            REQUIRE(recovered[i].category == whole_out[i].category);
    }

    // A cut right after the last record's final byte recovers all four; one byte short of
    // it drops exactly the trailing partial.
    const std::span<const std::byte> drop_last{full.data(), full.size() - 1};
    record_stream_reader             rd{drop_last};
    stream_definitions               dd;
    REQUIRE(rd.read_definitions(dd));
    std::vector<decoded_record> partial;
    const recovery_result       pr = rd.recover(partial);
    REQUIRE(pr.recovered == complete - 1);
    REQUIRE(pr.trailing_partial_dropped);
}
