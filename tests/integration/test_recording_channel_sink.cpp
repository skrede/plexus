#include "test_recording_channel_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recording_channel_fixture;

TEST_CASE("recording_channel stamps strictly monotonic independent per-direction sequences",
          "[recording_channel][wire]")
{
    auto                         *raw = new test_lower;
    recording_channel<test_lower> ch{std::unique_ptr<test_lower>(raw)};

    std::vector<std::uint64_t> out_seq;
    std::vector<std::uint64_t> in_seq;
    ch.on_wire([&](wire_direction dir, std::uint64_t seq, std::span<const std::byte>)
               { (dir == wire_direction::out ? out_seq : in_seq).push_back(seq); });
    // A self-loop: this channel's send feeds its own lower on_data, so each send produces
    // BOTH an OUT tap (the send counter) and an IN tap (the recv counter). The two counters
    // are independent, so each run is contiguous 0,1,2,... on its own axis.
    raw->m_sink = [raw](std::span<const std::byte> b) { raw->m_on_data(b); };

    for(int i = 0; i < 5; ++i)
        ch.send(std::span<const std::byte>{blob(static_cast<std::uint8_t>(i), 8)});

    const std::vector<std::uint64_t> expected{0, 1, 2, 3, 4};
    REQUIRE(out_seq == expected); // the OUT run is contiguous (no gaps)
    REQUIRE(in_seq == expected);  // the IN run is contiguous and independent
}

TEST_CASE("a wire_record reaches the recorder through recording_sink as a wire_frame",
          "[recording_channel][wire][recorder]")
{
    in_memory_byte_sink sink;
    std::uint64_t       tick = 0;
    flat_recorder       recorder{sink, 64u * 1024u, [&tick] { return ++tick; }};
    recorder.open(make_node(2), plexus::io::topic_capture_rule{});

    recording_sink tap{recorder};

    // Drive a captured frame through the decorator's tap into the sink's on_wire override,
    // exactly as the posted engine edge would (the tap builds a wire_record, on_wire records).
    auto                         *raw = new test_lower;
    recording_channel<test_lower> ch{std::unique_ptr<test_lower>(raw)};

    const node_id peer = make_node(9);
    ch.on_wire(
            [&](wire_direction dir, std::uint64_t seq, std::span<const std::byte> b)
            {
                const wire_record rec{dir, seq, peer, 0u, b};
                tap.on_wire(rec);
            });

    const auto frame = blob(0x30, 100);
    ch.send(std::span<const std::byte>{frame});

    while(recorder.pump())
        ;
    recorder.flush();

    record_stream_reader r{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    REQUIRE(r.recover(out).header_ok);

    bool saw_wire = false;
    for(const auto &rec : out)
        if(rec.category == record_category::wire_frame)
        {
            saw_wire = true;
            REQUIRE(rec.wire_dir == wire_direction::out);
            REQUIRE(rec.wire_seq == 0u);
            REQUIRE(rec.peer == peer);
            REQUIRE(std::vector<std::byte>(rec.payload.begin(), rec.payload.end()) == frame);
        }
    REQUIRE(saw_wire);
}
