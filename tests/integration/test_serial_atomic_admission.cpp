// The serial egress atomic-admission oracle: a sans-IO drive of serial_bootstrap::submit against a
// MOCK channel whose enqueue_egress* verbs forward to a REAL stream send_queue with a tiny byte cap
// and a recording sink (no pty, no socket). It pins the dangling-view UAF (an owner-carrying submit
// rejected by a full queue must never read the payload after the move, strongest under ASan) and
// the atomic framing (payload + CRC trailer enter the queue as one unit, so the wire carries only
// well-formed [header || payload || crc] frames — never a payload without its trailer).

#include "plexus/asio/detail/serial_bootstrap.h"

#include "plexus/stream/detail/send_queue.h"
#include "plexus/stream/crc_serial.h"

#include "plexus/wire_bytes.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

namespace {

namespace wire   = plexus::wire;
namespace stream = plexus::stream;

using send_queue = stream::detail::send_queue;

// Snapshots the gathered wire bytes per drain turn and parks each completion so the drain is driven
// by hand — the exactly-one-write-outstanding discipline.
struct rec_sink
{
    std::vector<std::vector<std::byte>> turns;
    std::vector<std::size_t>            views_per_turn;
    std::vector<send_queue::completion> pending;

    send_queue::send_sink sink()
    {
        return [this](send_queue::buffer_sequence views, send_queue::completion done)
        {
            std::vector<std::byte> flat;
            for(const auto &v : views)
                flat.insert(flat.end(), v.begin(), v.end());
            views_per_turn.push_back(views.size());
            turns.push_back(std::move(flat));
            pending.push_back(std::move(done));
        };
    }

    std::vector<std::byte> wire_stream() const
    {
        std::vector<std::byte> out;
        for(const auto &t : turns)
            out.insert(out.end(), t.begin(), t.end());
        return out;
    }
};

// A stand-in channel: the egress verbs serial_bootstrap can reach, each forwarding to the real
// send_queue and counting the full-queue rejections the production channel surfaces.
struct mock_channel
{
    send_queue queue;
    int        full_events{0};

    mock_channel(send_queue::send_sink s, std::size_t cap)
            : queue(std::move(s), cap)
    {
    }

    void enqueue_egress(std::span<const std::byte> bytes)
    {
        if(!queue.enqueue(bytes))
            ++full_events;
    }
    void enqueue_egress_owned(plexus::wire_bytes<> bytes)
    {
        if(!queue.enqueue(std::move(bytes)))
            ++full_events;
    }
    void enqueue_egress_pair(plexus::wire_bytes<> first, plexus::wire_bytes<> second)
    {
        if(!queue.enqueue_both(std::move(first), std::move(second)))
            ++full_events;
    }
};

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::vector<std::byte> framed_message(const std::string &payload)
{
    const wire::frame_header hdr{.type = wire::msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = 0};
    return wire::encode_frame(hdr, as_bytes(payload));
}

plexus::wire_bytes<> owned_of(const std::vector<std::byte> &bytes, std::shared_ptr<std::vector<std::byte>> &keep)
{
    keep = std::make_shared<std::vector<std::byte>>(bytes.begin(), bytes.end());
    return plexus::wire_bytes<>{std::span<const std::byte>{*keep}, keep};
}

// The peer's view of the wire: how many well-formed frames verified and how many torn spans dropped.
struct verify_result
{
    int                    matches{0};
    int                    drops{0};
    std::vector<std::byte> last_match;
};

verify_result verify_wire(std::span<const std::byte> bytes)
{
    verify_result r;
    stream::crc_serial_inbound dec;
    dec.on_match([&](std::span<const std::byte> f) { ++r.matches; r.last_match.assign(f.begin(), f.end()); });
    dec.on_drop([&](wire::close_cause) { ++r.drops; });
    dec.feed(bytes);
    return r;
}

}

TEST_CASE("serial submit admits payload and CRC trailer as one well-formed unit", "[integration][serial][atomic_admission]")
{
    // A byte cap of exactly the framed-payload size: the old two-admission logic admits the payload
    // and rejects the separate trailer, tearing the frame. The atomic admission treats them as one
    // message and admits both onto the empty queue — one verifiable [header || payload || crc] frame.
    const auto frame = framed_message("atomic-serial-payload");

    rec_sink                              rec;
    mock_channel                          ch{rec.sink(), frame.size()};
    plexus::asio::detail::serial_bootstrap<int> boot;

    std::shared_ptr<std::vector<std::byte>> keep;
    boot.submit(ch, owned_of(frame, keep));

    REQUIRE(ch.full_events == 0);            // the whole frame was admitted
    REQUIRE(rec.turns.size() == 1);          // driven in one turn
    REQUIRE(rec.views_per_turn[0] == 2);     // payload and trailer gathered together
    const auto wire = rec.wire_stream();
    REQUIRE(wire.size() == frame.size() + stream::crc_trailer_size);
    const auto seen = verify_wire(wire);
    REQUIRE(seen.matches == 1);              // the peer verifies exactly one frame
    REQUIRE(seen.drops == 0);                // nothing torn
    REQUIRE(seen.last_match == frame);       // header+payload delivered intact
}

TEST_CASE("serial submit admits the payload+trailer unit through the borrowed-view overload", "[integration][serial][atomic_admission]")
{
    const auto frame = framed_message("borrowed-view-payload");

    rec_sink                              rec;
    mock_channel                          ch{rec.sink(), frame.size()};
    plexus::asio::detail::serial_bootstrap<int> boot;

    boot.submit(ch, std::span<const std::byte>{frame});

    REQUIRE(ch.full_events == 0);
    REQUIRE(rec.views_per_turn.at(0) == 2);
    const auto seen = verify_wire(rec.wire_stream());
    REQUIRE(seen.matches == 1);
    REQUIRE(seen.drops == 0);
    REQUIRE(seen.last_match == frame);
}

TEST_CASE("serial submit is all-or-nothing on a full queue and never reads the moved-from payload", "[integration][serial][atomic_admission]")
{
    // The dangling-view UAF: with the queue full the payload admission is rejected, which under the
    // old path destroyed the moved-in owner then read its view for the CRC. The atomic admission
    // computes the trailer before the move and rejects both as a unit — no freed read (ASan-clean).
    const auto frame = framed_message("rejected-under-backpressure");

    rec_sink                              rec;
    mock_channel                          ch{rec.sink(), frame.size()};
    plexus::asio::detail::serial_bootstrap<int> boot;

    // Saturate the queue: a first frame fills the cap and parks in flight (its completion is held).
    std::vector<std::byte> filler(frame.size());
    ch.enqueue_egress(filler);
    REQUIRE(ch.queue.full());
    REQUIRE(rec.turns.size() == 1);

    // The payload's SOLE owner is the wire_bytes handed to submit, so a rejected move-in frees it.
    auto owner = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    plexus::wire_bytes<> payload{std::span<const std::byte>{*owner}, owner};
    owner.reset();
    boot.submit(ch, std::move(payload));

    REQUIRE(ch.full_events == 1);            // the frame was rejected as a unit
    REQUIRE(rec.turns.size() == 1);          // nothing new reached the wire
    REQUIRE(ch.queue.size() == 1);           // only the filler is queued
}
