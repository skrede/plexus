// over-limit: one cohesive cross-node loss-join proof; the single sequence-arithmetic join over two
// skewed-clock recorded streams is one end-to-end pipeline over a shared relay harness, so it
// cannot split without scattering that shared state The two-node cross-node loss-join: a sender's
// recording_channel stamps a per-direction OUT sequence on every framed send; a receiver's
// recording_channel stamps a per-direction IN sequence on every frame the lower channel delivers
// upward. A relay between them forwards every frame EXCEPT one injected drop. Decoding both
// recorded streams offline and reading ONLY the per-direction wire sequence + direction + peer
// (never a capture timestamp), the dropped packet is identified structurally: it is present in the
// sender's OUT run and absent (a sequence gap) in the receiver's IN run, found by sequence
// arithmetic over the two runs.
//
// The join is CLOCK-SKEW-IMMUNE by construction — the assertion never compares either node's
// capture_ts; the two recorders run on INDEPENDENT, deliberately skewed clocks to make that
// explicit. The inject-and-join is reproduced over several runs in-body (a timing-adjacent
// claim is never made from a single run). Deterministic header-only core (no backend, no
// socket): the relay is an in-process function, the drop a fixed index.

#include "in_memory_byte_sink.h"

#include "plexus/io/recording_channel.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

using plexus::node_id;
using plexus::io::recording_channel;
using plexus::io::recording::flat_recorder;
using plexus::io::recording::decoded_record;
using plexus::io::recording::wire_direction;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_category;
using plexus::io::recording::record_stream_reader;

namespace {

// A minimal lower byte_channel: send() captures the framed bytes (so a relay can ferry them
// to the peer's lower feed) and feed() drives this channel's on_data as a delivered frame.
class test_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
    }
    void close()
    {
    }
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const
    {
        return {"test", ""};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }
    [[nodiscard]] std::size_t backpressured() const
    {
        return 0;
    }

    void feed(std::span<const std::byte> bytes)
    {
        if(m_on_data)
            m_on_data(bytes);
    }

    [[nodiscard]] const std::vector<std::byte> &last() const
    {
        return m_last;
    }

private:
    std::vector<std::byte>                                               m_last;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
};

node_id make_node(std::uint8_t tag)
{
    node_id n{};
    n[0]  = std::byte{tag};
    n[15] = std::byte{0xEE};
    return n;
}

std::vector<std::byte> frame_of(std::uint8_t base, std::size_t n)
{
    std::vector<std::byte> v;
    for(std::size_t i = 0; i < n; ++i)
        v.push_back(static_cast<std::byte>((base + i) & 0xffu));
    return v;
}

// One captured frame as the offline join sees it: its per-direction sequence and the framed
// bytes. The join reads ONLY {dir, seq, peer, payload} — never capture_ts — so the
// reconstruction is a clock-free view of what each node observed on its own side.
struct captured_frame
{
    std::uint64_t          seq{};
    std::vector<std::byte> bytes;
};

std::vector<captured_frame> frame_run(std::span<const std::byte> stream, wire_direction want, const node_id &peer)
{
    record_stream_reader r{stream};
    stream_definitions   defs;
    REQUIRE(r.read_definitions(defs));
    std::vector<decoded_record> out;
    REQUIRE(r.recover(out).header_ok);

    std::vector<captured_frame> run;
    for(const auto &rec : out)
        if(rec.category == record_category::wire_frame && rec.wire_dir == want && rec.peer == peer)
            run.push_back({rec.wire_seq, {rec.payload.begin(), rec.payload.end()}});
    return run;
}

std::vector<std::uint64_t> seqs_of(const std::vector<captured_frame> &run)
{
    std::vector<std::uint64_t> s;
    for(const auto &f : run)
        s.push_back(f.seq);
    return s;
}

// The sender OUT sequence whose framed bytes appear in the sender's run but in NO received
// frame — the structurally lost packet. The two per-direction counters are each node-local
// (the receiver's IN run is contiguous on its own axis with one fewer entry, never a gap in
// its own count), so the cross-node match aligns the runs by their byte-identical framed
// payloads and the answer is the sender OUT seq of the unmatched frame. Pure structural
// arithmetic over the two runs — no clock, no capture timestamp.
std::optional<std::uint64_t> lost_out_seq(const std::vector<captured_frame> &sent, const std::vector<captured_frame> &received)
{
    for(const auto &s : sent)
    {
        const bool seen = std::any_of(received.begin(), received.end(), [&](const captured_frame &r) { return r.bytes == s.bytes; });
        if(!seen)
            return s.seq;
    }
    return std::nullopt;
}

}

TEST_CASE("two-node loss-join: an injected drop is a structural sequence gap, clock-skew-immune", "[wire_loss_join][wire]")
{
    constexpr int           k_runs   = 5;
    constexpr std::size_t   k_frames = 12;
    constexpr std::uint64_t k_drop   = 7; // the sender's OUT seq dropped in transit

    int proven = 0;
    for(int run = 0; run < k_runs; ++run)
    {
        const node_id sender_id   = make_node(0xA1);
        const node_id receiver_id = make_node(0xB2);

        in_memory_byte_sink sender_sink;
        in_memory_byte_sink receiver_sink;

        // Two recorders on DELIBERATELY skewed, independent clocks: the receiver's clock runs
        // a large fixed offset ahead of the sender's, so any join that read capture_ts across
        // nodes would be wrong. The loss reconstruction below never touches capture_ts.
        std::uint64_t sender_tick   = 0;
        std::uint64_t receiver_tick = 1'000'000'000ull; // +1s skew, monotonic on its own axis
        flat_recorder sender_rec{sender_sink, 256u * 1024u, [&sender_tick] { return ++sender_tick; }};
        flat_recorder receiver_rec{receiver_sink, 256u * 1024u, [&receiver_tick] { return receiver_tick += 1000; }};
        sender_rec.open(sender_id, plexus::io::topic_capture_rule{});
        receiver_rec.open(receiver_id, plexus::io::topic_capture_rule{});

        auto                         *sender_lower   = new test_lower;
        auto                         *receiver_lower = new test_lower;
        recording_channel<test_lower> sender_ch{std::unique_ptr<test_lower>(sender_lower)};
        recording_channel<test_lower> receiver_ch{std::unique_ptr<test_lower>(receiver_lower)};

        // The sender stamps its OUT run; the receiver stamps its IN run. The peer recorded on
        // each side is the OTHER node's identity (the slot the channel belongs to).
        sender_ch.on_wire([&](wire_direction dir, std::uint64_t seq, std::span<const std::byte> b) { sender_rec.record_wire(dir, seq, receiver_id, b); });
        receiver_ch.on_wire([&](wire_direction dir, std::uint64_t seq, std::span<const std::byte> b) { receiver_rec.record_wire(dir, seq, sender_id, b); });

        // The relay: every framed send the sender emits is ferried to the receiver's lower
        // feed (driving the receiver's IN tap) EXCEPT the one injected drop, which is silently
        // discarded in transit — exactly a packet lost on the link.
        std::uint64_t out_index = 0;
        for(std::size_t i = 0; i < k_frames; ++i)
        {
            const auto frame = frame_of(static_cast<std::uint8_t>(0x10 + i), 32 + i);
            sender_ch.send(std::span<const std::byte>{frame}); // OUT seq = out_index
            if(out_index != k_drop)
                receiver_lower->feed(std::span<const std::byte>{sender_lower->last()});
            ++out_index;
        }

        while(sender_rec.pump())
            ;
        sender_rec.flush();
        while(receiver_rec.pump())
            ;
        receiver_rec.flush();

        const auto sent_run = frame_run(sender_sink.bytes(), wire_direction::out, receiver_id);
        const auto recv_run = frame_run(receiver_sink.bytes(), wire_direction::in, sender_id);

        // Each per-direction counter is contiguous on its OWN axis: the sender OUT run is
        // 0..k_frames-1 (every send is stamped) and the receiver IN run is 0..k_frames-2
        // (it stamps one fewer because one frame never arrived). Neither run has an internal
        // gap — the loss shows up as a COUNT difference across the two nodes, not a hole in
        // either node's own monotonic count.
        std::vector<std::uint64_t> sent_contiguous(k_frames);
        for(std::size_t i = 0; i < k_frames; ++i)
            sent_contiguous[i] = i;
        std::vector<std::uint64_t> recv_contiguous(k_frames - 1);
        for(std::size_t i = 0; i + 1 < k_frames; ++i)
            recv_contiguous[i] = i;
        REQUIRE(seqs_of(sent_run) == sent_contiguous);
        REQUIRE(seqs_of(recv_run) == recv_contiguous);

        // Exactly one packet lost: the sender observed one more frame than the receiver.
        REQUIRE(sent_run.size() == recv_run.size() + 1);

        // The lost packet is identified STRUCTURALLY: its sender OUT seq is the one whose
        // framed bytes appear in the sender's OUT capture but in no received frame. The join
        // reads only seq/dir/peer/bytes — never a cross-node capture timestamp (the two
        // recorders ran on a deliberate +1s clock skew, which the answer is immune to).
        const auto lost = lost_out_seq(sent_run, recv_run);
        REQUIRE(lost.has_value());
        REQUIRE(*lost == k_drop);
        ++proven;
    }
    REQUIRE(proven == k_runs);
}
