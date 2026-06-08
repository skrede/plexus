// The stream_send_queue block oracle: a pure sans-IO drive of the endpoint-less serial
// outbound discipline with a recording stub send-sink (capturing the owned bytes + a
// manual completion trigger), no socket and no backend link (plexus::plexus only). It is
// the stream sibling of send_queue: the datagram block carries a per-node Endpoint, this
// one carries none (the stream sink is async_write(socket/stream, buffer) with no
// destination). It proves the same lifetime/order disciplines the asio + tls stream
// channels relied on — copy-into-owned-node, one-outstanding serial drain, FIFO across a
// burst, close() clearing a pending queue and guarding a late completion — PLUS the
// bounded-capacity surface (fill-to-cap, the at-capacity signal fires, no further admit
// until a drain frees room) AND the fail-on-error edge (a channel that fails on a socket
// error closes the block so the post-close completion does not chain — the stream's
// fail-before-chain semantics, not a swallow).

#include "plexus/io/detail/stream_send_queue.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <utility>

using stream_send_queue = plexus::io::detail::stream_send_queue;

namespace {

// A recording sink: snapshots each presented bytes view and parks the completion so the
// test drives the serial drain by hand (proving exactly-one-outstanding).
struct recorder
{
    std::vector<std::vector<std::byte>> calls;
    std::vector<stream_send_queue::completion> pending;

    stream_send_queue::send_sink sink()
    {
        return [this](std::span<const std::byte> bytes, stream_send_queue::completion done)
        {
            calls.emplace_back(bytes.begin(), bytes.end());
            pending.push_back(std::move(done));
        };
    }

    void complete_front(bool ok = true)
    {
        auto done = std::move(pending.front());
        pending.erase(pending.begin());
        done(ok);
    }
};

std::vector<std::byte> bytes_of(std::initializer_list<int> vals)
{
    std::vector<std::byte> out;
    for(int v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

TEST_CASE("stream_send_queue copies caller bytes into an owned node on enqueue", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink()};

    auto scratch = bytes_of({1, 2, 3});
    q.enqueue(scratch);

    // Mutate the caller's scratch AFTER enqueue: the node must hold its own copy.
    scratch[0] = static_cast<std::byte>(99);

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0] == bytes_of({1, 2, 3}));
}

TEST_CASE("stream_send_queue keeps at most one send-sink invocation outstanding", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));
    q.enqueue(bytes_of({2}));
    q.enqueue(bytes_of({3}));

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(q.sending());

    rec.complete_front();
    REQUIRE(rec.calls.size() == 2);

    rec.complete_front();
    REQUIRE(rec.calls.size() == 3);
}

TEST_CASE("stream_send_queue drains in FIFO order across a burst", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({10}));
    q.enqueue(bytes_of({20}));
    q.enqueue(bytes_of({30}));

    rec.complete_front();
    rec.complete_front();

    REQUIRE(rec.calls.size() == 3);
    REQUIRE(rec.calls[0] == bytes_of({10}));
    REQUIRE(rec.calls[1] == bytes_of({20}));
    REQUIRE(rec.calls[2] == bytes_of({30}));

    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue close() clears a pending queue and guards a late completion", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));
    q.enqueue(bytes_of({2}));
    REQUIRE(q.size() == 2);

    q.close();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());

    // A completion arriving after close is a guarded no-op: it must not chain or pop.
    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE(rec.calls.size() == 1);
}

TEST_CASE("stream_send_queue default capacity is unbounded — the at-capacity signal is inert", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink()};

    REQUIRE(q.full() == false);
    for(int i = 0; i < 1000; ++i)
        REQUIRE(q.enqueue(bytes_of({i & 0xff})));
    REQUIRE(q.full() == false);
    REQUIRE(q.size() == 1000);
}

TEST_CASE("stream_send_queue bounded capacity fires the at-capacity signal and refuses past the cap", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink(), 2};

    REQUIRE(q.enqueue(bytes_of({1})));       // admitted, now in flight (1 byte)
    REQUIRE(q.enqueue(bytes_of({2})));       // admitted, parked behind the front (2 bytes)
    REQUIRE(q.full());

    REQUIRE_FALSE(q.enqueue(bytes_of({3}))); // refused at capacity, no node added
    REQUIRE(q.size() == 2);

    rec.complete_front();                    // a drain frees one slot; admission resumes
    REQUIRE(q.size() == 1);
    REQUIRE_FALSE(q.full());
    REQUIRE(q.enqueue(bytes_of({3})));
    REQUIRE(q.full());
    REQUIRE(q.size() == 2);
}

TEST_CASE("stream_send_queue caps on summed BYTES, not entry count", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink(), 8};

    REQUIRE(q.enqueue(bytes_of({1, 2, 3, 4, 5})));   // one ENTRY, 5 of 8 bytes
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 5);
    REQUIRE_FALSE(q.full());

    REQUIRE(q.enqueue(bytes_of({6, 7, 8})));         // exactly to the byte cap (5 + 3 == 8)
    REQUIRE(q.queued_bytes() == 8);
    REQUIRE(q.full());

    REQUIRE_FALSE(q.enqueue(bytes_of({9})));         // refused on BYTES, count small
    REQUIRE(q.size() == 2);
    REQUIRE(q.queued_bytes() == 8);
}

TEST_CASE("stream_send_queue near-cap boundary: byte accounting does not wrap and refuses correctly", "[io][stream_send_queue]")
{
    // Overflow boundary: a frame at cap-1 bytes followed by a small frame whose sum
    // exceeds the cap must be refused (compare-before-add), and the running total must
    // NOT wrap below the cap and re-admit. Cap = 16; first frame = 15 bytes (cap-1).
    recorder rec;
    stream_send_queue q{rec.sink(), 16};

    REQUIRE(q.enqueue(std::vector<std::byte>(15)));   // 15 of 16 bytes
    REQUIRE(q.queued_bytes() == 15);
    REQUIRE_FALSE(q.full());

    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(2)));   // 17 > 16 — refused, no wrap
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 15);

    REQUIRE(q.enqueue(std::vector<std::byte>(1)));    // the remaining byte fits exactly
    REQUIRE(q.queued_bytes() == 16);
    REQUIRE(q.full());

    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(1)));
    REQUIRE(q.queued_bytes() == 16);
}

TEST_CASE("stream_send_queue fail-on-error edge: a composer that closes the block stops the chain", "[io][stream_send_queue]")
{
    // The stream channel fails the channel on a socket error: it closes the block BEFORE
    // the completion would chain, so the post-close completion is a guarded no-op (the
    // next queued frame is NOT written through the failed socket). This pins the
    // fail-before-chain semantics — the error is surfaced to the composer, never swallowed.
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));
    q.enqueue(bytes_of({2}));
    REQUIRE(rec.calls.size() == 1);          // only the front is in flight

    // Simulate the composer's fail path: on the error completion it closes the block.
    q.close();
    rec.complete_front(/*ok=*/false);        // the failed write's completion fires post-close

    REQUIRE(rec.calls.size() == 1);          // the second frame was NOT chained onto the failed socket
    REQUIRE(q.size() == 0);
}
