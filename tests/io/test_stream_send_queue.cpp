// The stream_send_queue block oracle: a pure sans-IO drive of the endpoint-less serial
// outbound discipline with a recording stub send-sink (capturing the gathered bytes + a
// manual completion trigger), no socket and no backend link (plexus::plexus only). It is
// the stream sibling of send_queue: the datagram block carries a per-node Endpoint, this
// one carries none (the stream sink is one async_write over a buffer SEQUENCE with no
// destination). It proves the same lifetime/order disciplines the asio + tls stream
// channels relied on — copy-into-owned-node, one-outstanding serial drain, FIFO across a
// burst, close() clearing a pending queue and guarding a late completion — PLUS the
// gather-write reshape (a drain turn coalesces the front-N nodes into ONE sink call over
// N views), the wire_bytes owner-handle node (the supplied owner stays alive across the
// single completion, no per-frame copy on the owner path), the bounded-capacity surface
// (fill-to-cap, the at-capacity signal fires, no further admit until a drain frees room),
// AND the fail-on-error edge (a channel that fails on a socket error closes the block so
// the post-close completion does not chain — the stream's fail-before-chain semantics).

#include "plexus/io/detail/stream_send_queue.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

using stream_send_queue = plexus::io::detail::stream_send_queue;

namespace {

// A recording sink: snapshots the SEQUENCE of bytes views presented per drain turn (one
// inner vector per gathered node) and parks the completion so the test drives the serial
// drain by hand (proving exactly-one-write-outstanding). The pointer of the first view is
// also recorded so a test can assert the owner path passes a view, not a copy.
struct recorder
{
    std::vector<std::vector<std::vector<std::byte>>> calls;   // [turn][node][bytes]
    std::vector<const std::byte *> first_view_ptr;            // [turn] -> &views[0][0]
    std::vector<stream_send_queue::completion> pending;

    stream_send_queue::send_sink sink()
    {
        return [this](stream_send_queue::buffer_sequence views, stream_send_queue::completion done)
        {
            std::vector<std::vector<std::byte>> turn;
            for(const auto &v : views)
                turn.emplace_back(v.begin(), v.end());
            first_view_ptr.push_back(views.empty() ? nullptr : views.front().data());
            calls.push_back(std::move(turn));
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

    REQUIRE(rec.calls.size() == 1);          // one drain turn
    REQUIRE(rec.calls[0].size() == 1);       // one gathered node
    REQUIRE(rec.calls[0][0] == bytes_of({1, 2, 3}));
}

TEST_CASE("stream_send_queue keeps at most one send-sink invocation outstanding", "[io][stream_send_queue]")
{
    // The first enqueue drives immediately and alone; the next two park behind the
    // in-flight turn and gather into the NEXT turn. At most ONE turn is outstanding at a
    // time (the serial discipline), and the parked frames coalesce into a single call.
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));                // turn 0: in flight alone
    q.enqueue(bytes_of({2}));
    q.enqueue(bytes_of({3}));                // {2,3} parked

    REQUIRE(rec.calls.size() == 1);          // one turn outstanding
    REQUIRE(rec.calls[0].size() == 1);
    REQUIRE(q.sending());

    rec.complete_front();                    // turn 1 gathers {2,3} into ONE call
    REQUIRE(rec.calls.size() == 2);
    REQUIRE(rec.calls[1].size() == 2);

    rec.complete_front();
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue drains in FIFO order across a burst", "[io][stream_send_queue]")
{
    // Enqueue while a turn is in flight: the in-flight front gathers alone, the later
    // frames gather into the NEXT turn in FIFO order behind it.
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({10}));               // turn 0: in flight alone
    q.enqueue(bytes_of({20}));               // parked behind the in-flight turn
    q.enqueue(bytes_of({30}));

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0] == std::vector<std::vector<std::byte>>{bytes_of({10})});

    rec.complete_front();                    // turn 0 done; turn 1 gathers {20, 30}
    REQUIRE(rec.calls.size() == 2);
    REQUIRE(rec.calls[1] == std::vector<std::vector<std::byte>>{bytes_of({20}), bytes_of({30})});

    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue close() clears a pending queue and guards a late completion", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));    // gathers into the in-flight turn
    q.enqueue(bytes_of({2}));    // rides the same gather turn
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

namespace {

// A wire_bytes whose owner is a heap vector the test can observe: the owner is the
// std::shared_ptr keeping the bytes alive. use_count() witnesses that the block holds a
// reference to the owner across the in-flight write (the owner-lifetime invariant), and
// the view's data() witnesses no intermediate copy (the queue passes the owner's bytes).
plexus::wire_bytes<> owned_frame(std::initializer_list<int> vals,
                                 std::shared_ptr<std::vector<std::byte>> &keep)
{
    auto buf = std::make_shared<std::vector<std::byte>>();
    for(int v : vals)
        buf->push_back(static_cast<std::byte>(v));
    keep = buf;
    return plexus::wire_bytes<>{std::span<const std::byte>{*buf}, buf};
}

}

TEST_CASE("stream_send_queue gathers a multi-frame burst into ONE drain turn over N views", "[io][stream_send_queue]")
{
    // The gather-write: a drain turn with N>1 queued frames issues exactly ONE sink call
    // carrying a SEQUENCE of N views, not N separate calls. The first frame drives
    // alone; the rest gather into the next turn behind it.
    recorder rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));            // turn 0, in flight alone
    q.enqueue(bytes_of({2}));
    q.enqueue(bytes_of({3}));
    q.enqueue(bytes_of({4}));            // {2,3,4} park behind the in-flight turn

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0].size() == 1);

    rec.complete_front();                // turn 1 gathers {2,3,4} into ONE call
    REQUIRE(rec.calls.size() == 2);
    REQUIRE(rec.calls[1].size() == 3);   // exactly one sink call for three frames
    REQUIRE(rec.calls[1][0] == bytes_of({2}));
    REQUIRE(rec.calls[1][1] == bytes_of({3}));
    REQUIRE(rec.calls[1][2] == bytes_of({4}));

    rec.complete_front();
    REQUIRE(q.size() == 0);
}

TEST_CASE("stream_send_queue bounds the gather count per drain turn", "[io][stream_send_queue]")
{
    // The gather is bounded: at most gather_limit frames coalesce per turn, so a deep
    // backlog drains in ceil(backlog / limit) turns, never one unbounded iovec.
    recorder rec;
    stream_send_queue q{rec.sink(), stream_send_queue::unbounded, /*gather_limit=*/3};

    for(int i = 0; i < 7; ++i)
        q.enqueue(bytes_of({i}));        // {0} in flight alone, {1..6} parked

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0].size() == 1);

    rec.complete_front();                // turn 1: gathers {1,2,3} (capped at 3)
    REQUIRE(rec.calls[1].size() == 3);

    rec.complete_front();                // turn 2: gathers {4,5,6}
    REQUIRE(rec.calls[2].size() == 3);

    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue holds a wire_bytes owner with no copy and keeps it alive across the completion",
          "[io][stream_send_queue]")
{
    // The owner-handle path: enqueue(wire_bytes) holds the supplied owner and
    // passes its VIEW (no intermediate copy — the presented pointer equals the owner's
    // bytes), and the owner stays referenced by the block until the SINGLE completion
    // fires (releasing it before would be a read-after-free under a real gather-write).
    recorder rec;
    stream_send_queue q{rec.sink()};

    std::shared_ptr<std::vector<std::byte>> keep;
    auto frame = owned_frame({7, 8, 9}, keep);
    const std::byte *owner_bytes = frame.data();
    REQUIRE(keep.use_count() == 2);                  // the test's keep + the frame's owner

    q.enqueue(std::move(frame));

    // The view handed to the sink is the OWNER's bytes, not a copy.
    REQUIRE(rec.first_view_ptr.size() == 1);
    REQUIRE(rec.first_view_ptr[0] == owner_bytes);
    REQUIRE(rec.calls[0][0] == bytes_of({7, 8, 9}));

    // The owner is still referenced by the in-flight node (use_count climbed): the block
    // holds it alive across the write. Dropping the test's local keep must NOT free it.
    REQUIRE(keep.use_count() >= 2);
    auto weak = std::weak_ptr<std::vector<std::byte>>(keep);
    keep.reset();
    REQUIRE_FALSE(weak.expired());                   // the block keeps the owner alive in flight

    rec.complete_front();                            // the single completion releases the owner
    REQUIRE(weak.expired());                         // released only AFTER the completion
    REQUIRE(q.size() == 0);
}

TEST_CASE("stream_send_queue keeps ALL gathered owners alive until the single completion",
          "[io][stream_send_queue]")
{
    // The gather-write hazard: ONE completion for N frames, so ALL N owners must outlive
    // it. Gather three owner frames into one turn, drop every external reference, and
    // assert none is freed until the single completion fires.
    recorder rec;
    stream_send_queue q{rec.sink()};

    // A first frame drives alone; three owner frames then gather into the next turn.
    q.enqueue(bytes_of({0}));

    std::shared_ptr<std::vector<std::byte>> k1, k2, k3;
    auto f1 = owned_frame({1}, k1);
    auto f2 = owned_frame({2}, k2);
    auto f3 = owned_frame({3}, k3);
    std::weak_ptr<std::vector<std::byte>> w1{k1}, w2{k2}, w3{k3};

    q.enqueue(std::move(f1));
    q.enqueue(std::move(f2));
    q.enqueue(std::move(f3));
    k1.reset(); k2.reset(); k3.reset();              // drop every external reference

    rec.complete_front();                            // turn 0 done; the three gather into turn 1
    REQUIRE(rec.calls[1].size() == 3);
    REQUIRE_FALSE(w1.expired());                     // all three owners alive in flight
    REQUIRE_FALSE(w2.expired());
    REQUIRE_FALSE(w3.expired());

    rec.complete_front();                            // the single completion releases all three
    REQUIRE(w1.expired());
    REQUIRE(w2.expired());
    REQUIRE(w3.expired());
    REQUIRE(q.size() == 0);
}
