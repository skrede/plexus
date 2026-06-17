// The flat record-stream format oracle. It proves the canonical encoding round-trips
// through the reader (mixed categories, a payload sample byte-identical with no codec in
// the path), that the envelope union RESERVES the wire-record variant now (forward-stable
// format), that a mid-write truncation recovers every complete record and loses only the
// trailing partial (crash-recovery by construction), and that the recording_sink ->
// flat_recorder -> byte_ring -> byte_sink chain captures synthetic observer edges that
// decode offline. The in-memory byte_sink is the non-disk drain target; a codec is
// supplied only in the test to interpret a sample's payload, proving the stream itself
// carries raw bytes.

#include "in_memory_byte_sink.h"

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/recording_sink.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"
#include "plexus/io/recording/record_stream_writer.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <type_traits>

using plexus::node_id;
using plexus::io::message_info;
using plexus::io::endpoint_edge;
using plexus::io::endpoint_event;
using plexus::io::capture_fidelity;
using plexus::io::topic_capture_rule;
using plexus::io::detail::drop_event;
using plexus::io::detail::drop_cause;
using plexus::io::recording::byte_ring;
using plexus::io::recording::wire_record;
using plexus::io::recording::wire_direction;
using plexus::io::recording::flat_recorder;
using plexus::io::recording::recording_sink;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::type_schema_entry;
using plexus::io::recording::record_stream_reader;
using plexus::io::recording::record_stream_writer;

namespace {

node_id make_node(std::uint8_t tag)
{
    node_id n{};
    n[0] = std::byte{tag};
    n[15] = std::byte{0xAB};
    return n;
}

std::span<const std::byte> bytes_of(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Encode a complete in-memory stream of mixed-category records through a real byte_ring
// drain into an in-memory sink. Returns the framed byte stream the reader consumes.
std::vector<std::byte> encode_mixed_stream(record_stream_writer &w, byte_ring &ring,
                                           in_memory_byte_sink &sink,
                                           const std::string &payload)
{
    sink.write(w.begin_stream(7u, make_node(1), topic_capture_rule{}, {}));

    ring.try_push(w.sync_marker());

    message_info info{};
    info.publication_sequence = 42;
    info.source_timestamp     = 100;
    info.reception_timestamp   = 101;
    ring.try_push(w.sample(11u, plexus::wire::fqn_topic_hash("topic/a"), info, 0xC0DEu, true,
                           capture_fidelity::payload, bytes_of(payload)));

    message_info meta{};
    meta.publication_sequence = 7;
    ring.try_push(w.sample(12u, plexus::wire::fqn_topic_hash("topic/b"), meta, 0u, false,
                           capture_fidelity::metadata, {}));

    drop_event de{};
    de.cause      = drop_cause::drop_newest;
    de.topic_hash = plexus::wire::fqn_topic_hash("topic/a");
    de.count      = 5;
    ring.try_push(w.drop(13u, de));

    endpoint_event ee{};
    ee.edge       = endpoint_edge::publisher_declared;
    ee.topic_hash = plexus::wire::fqn_topic_hash("topic/a");
    ee.type_id    = 0xC0DEu;
    ring.try_push(w.endpoint(14u, "topic/a", ee));

    while(ring.drain(sink, 4096))
        ;

    return {sink.bytes().begin(), sink.bytes().end()};
}

}

TEST_CASE("record stream reserves the wire-record variant", "[record_stream]")
{
    // Forward-stable format: the wire variant is defined NOW (the wire-fidelity tier adds
    // only the byte source later, with zero format change). Nothing populates it here.
    STATIC_REQUIRE(static_cast<std::uint8_t>(record_category::wire_frame) != 0);
    REQUIRE(record_category::wire_frame != record_category::sample);
    REQUIRE(record_category::wire_frame != record_category::dropout);
}

TEST_CASE("record stream populates the wire variant with no format bump", "[record_stream][wire]")
{
    // The wire tier reuses ordinal 11 and the version stays 1 — populating the slot adds
    // only the byte source, never a version bump or an ordinal reorder.
    STATIC_REQUIRE(static_cast<std::uint8_t>(record_category::wire_frame) == 11);
    STATIC_REQUIRE(plexus::io::recording::k_format_version == 1u);
}

TEST_CASE("record stream round-trips a wire frame byte-identically offline", "[record_stream][wire]")
{
    record_stream_writer w;
    byte_ring            ring{64u * 1024u};
    in_memory_byte_sink  sink;

    sink.write(w.begin_stream(5u, make_node(3), topic_capture_rule{}, {}));
    ring.try_push(w.sync_marker());

    // A header-on framed blob captured verbatim — opaque bytes, no codec in the path.
    std::vector<std::byte> frame;
    for(std::uint32_t i = 0; i < 200; ++i)
        frame.push_back(static_cast<std::byte>(i & 0xffu));

    const node_id peer = make_node(9);
    ring.try_push(w.wire_frame(77u, wire_direction::out, 42u, peer,
                               std::span<const std::byte>{frame}));
    ring.try_push(w.wire_frame(78u, wire_direction::in, 43u, peer,
                               std::span<const std::byte>{frame}));

    while(ring.drain(sink, 4096))
        ;

    record_stream_reader        r{sink.bytes()};
    stream_definitions          defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    const recovery_result       res = r.recover(out);
    REQUIRE(res.header_ok);
    REQUIRE_FALSE(res.corruption_skipped);
    REQUIRE(out.size() == 2);

    REQUIRE(out[0].category == record_category::wire_frame);
    REQUIRE(out[0].wire_dir == wire_direction::out);
    REQUIRE(out[0].wire_seq == 42u);
    REQUIRE(out[0].peer == peer);
    REQUIRE(out[0].capture_ts == 77u);
    REQUIRE(std::vector<std::byte>(out[0].payload.begin(), out[0].payload.end()) == frame);

    REQUIRE(out[1].category == record_category::wire_frame);
    REQUIRE(out[1].wire_dir == wire_direction::in);
    REQUIRE(out[1].wire_seq == 43u);
    REQUIRE(std::vector<std::byte>(out[1].payload.begin(), out[1].payload.end()) == frame);
}

TEST_CASE("record stream interleaves a wire frame with the other categories", "[record_stream][wire]")
{
    record_stream_writer w;
    byte_ring            ring{64u * 1024u};
    in_memory_byte_sink  sink;

    sink.write(w.begin_stream(1u, make_node(4), topic_capture_rule{}, {}));
    ring.try_push(w.sync_marker());

    message_info info{};
    info.publication_sequence = 3;
    ring.try_push(w.sample(11u, plexus::wire::fqn_topic_hash("topic/a"), info, 0u, false,
                           capture_fidelity::payload, bytes_of(std::string{"payload"})));

    std::vector<std::byte> frame{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
    ring.try_push(w.wire_frame(2u, wire_direction::out, 1u, make_node(9),
                               std::span<const std::byte>{frame}));

    drop_event de{};
    de.cause = drop_cause::drop_newest;
    de.count = 2;
    ring.try_push(w.drop(3u, de));

    while(ring.drain(sink, 4096))
        ;

    record_stream_reader        r{sink.bytes()};
    stream_definitions          defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    const recovery_result       res = r.recover(out);
    REQUIRE(res.header_ok);
    REQUIRE_FALSE(res.trailing_partial_dropped);
    REQUIRE(out.size() == 3);
    REQUIRE(out[0].category == record_category::sample);
    REQUIRE(out[1].category == record_category::wire_frame);
    REQUIRE(std::vector<std::byte>(out[1].payload.begin(), out[1].payload.end()) == frame);
    REQUIRE(out[2].category == record_category::drop);

    // Truncating one byte short of the end drops only the trailing partial; the wire frame
    // ahead of it survives the recovery scan.
    const std::span<const std::byte> all{sink.bytes()};
    record_stream_reader             rt{all.first(all.size() - 1)};
    stream_definitions               td;
    REQUIRE(rt.read_definitions(td));
    std::vector<decoded_record> trunc;
    const recovery_result       tr = rt.recover(trunc);
    REQUIRE(tr.trailing_partial_dropped);
    REQUIRE(trunc.size() == 2);
    REQUIRE(trunc[1].category == record_category::wire_frame);
}

TEST_CASE("record_wire over a saturated ring sheds at the wire tier", "[record_stream][wire][recorder]")
{
    in_memory_byte_sink sink;
    std::uint64_t       tick = 0;
    // A tiny ring so a couple of wire frames overflow it before a drain runs.
    flat_recorder       recorder{sink, 256u, [&tick] { return ++tick; }};
    recorder.open(make_node(5), topic_capture_rule{});

    const node_id          peer = make_node(9);
    std::vector<std::byte> big(200, std::byte{0x5A});

    // Fill then overflow: the ~240-byte framed records exceed the 256-byte ring one at a
    // time, so every admit past the first shed into the dropout run (drop-newest).
    for(int i = 0; i < 8; ++i)
        recorder.record_wire(wire_direction::out, static_cast<std::uint64_t>(i), peer,
                             std::span<const std::byte>{big});

    // Drain to free the ring, then a small admit surfaces the accumulated dropout_record
    // BEFORE it (the gap is quantified on the first successful admit after a shed run).
    while(recorder.pump())
        ;
    std::vector<std::byte> small{std::byte{0x01}};
    recorder.record_wire(wire_direction::in, 99u, peer, std::span<const std::byte>{small});

    while(recorder.pump())
        ;
    recorder.flush();

    record_stream_reader        r{sink.bytes()};
    stream_definitions          defs;
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

    record_stream_reader        r{stream};
    stream_definitions          defs;
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
    const std::string decoded{reinterpret_cast<const char *>(out[0].payload.data()),
                              out[0].payload.size()};
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

    record_stream_reader        whole{full};
    stream_definitions          wd;
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

    const std::string         body = "imu-sample-bytes";
    const plexus::io::message_view view{bytes_of(body), {}};
    message_info              info{};
    info.publication_sequence = 9;
    tap.on_message_delivered("sensor/imu", info, view);

    while(recorder.pump())
        ;
    recorder.flush();

    record_stream_reader        r{sink.bytes()};
    stream_definitions          defs;
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
    const std::string got{reinterpret_cast<const char *>(out[2].payload.data()),
                          out[2].payload.size()};
    REQUIRE(got == body);
}
