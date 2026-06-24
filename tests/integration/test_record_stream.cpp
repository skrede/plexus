#include "test_record_stream_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace record_stream_fixture;

TEST_CASE("record stream reserves the wire-record variant", "[record_stream]")
{
    // Forward-stable format: the wire variant is defined NOW (the wire-fidelity tier adds
    // only the byte source later, with zero format change). Nothing populates it here.
    STATIC_REQUIRE(static_cast<std::uint8_t>(record_category::wire_frame) != 0);
    REQUIRE(record_category::wire_frame != record_category::sample);
    REQUIRE(record_category::wire_frame != record_category::dropout);
}

TEST_CASE("record stream populates the wire variant with no ordinal reorder", "[record_stream][wire]")
{
    // The wire tier reuses ordinal 11 — populating the slot adds only the byte source, never
    // an ordinal reorder. The format version reflects the self-describing preamble layout, not
    // the wire-record category, which the reserved slot folded in without a bump.
    STATIC_REQUIRE(static_cast<std::uint8_t>(record_category::wire_frame) == 11);
    STATIC_REQUIRE(plexus::io::recording::k_format_version == 2u);
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
    ring.try_push(w.wire_frame(77u, wire_direction::out, 42u, peer, std::span<const std::byte>{frame}));
    ring.try_push(w.wire_frame(78u, wire_direction::in, 43u, peer, std::span<const std::byte>{frame}));

    while(ring.drain(sink, 4096))
        ;

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
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
    ring.try_push(w.sample(11u, plexus::wire::fqn_topic_hash("topic/a"), info, 0u, false, capture_fidelity::payload, bytes_of(std::string{"payload"})));

    std::vector<std::byte> frame{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};
    ring.try_push(w.wire_frame(2u, wire_direction::out, 1u, make_node(9), std::span<const std::byte>{frame}));

    drop_event de{};
    de.cause = drop_cause::drop_newest;
    de.count = 2;
    ring.try_push(w.drop(3u, de));

    while(ring.drain(sink, 4096))
        ;

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
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
