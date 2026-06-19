// The send_queue block oracle: a pure sans-IO drive of the generic serial outbound
// discipline with a recording stub send-sink (capturing the owned bytes + a manual
// completion trigger), no socket and no backend link (plexus::plexus only). It proves
// the four lifetime/order disciplines the asio socket relied on — copy-into-owned-node,
// one-outstanding serial drain, FIFO across a burst, close() clearing a pending queue —
// PLUS the bounded-capacity surface (fill-to-cap, the at-capacity signal fires, no
// further admit until a drain frees room) that a capped caller reuses. The destination
// type is a plain int here (the block is generic over the endpoint), proving the block
// carries no UDP shape.

#include "plexus/io/detail/send_queue.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <utility>

using send_queue = plexus::io::detail::send_queue<int>;

namespace {

// A recording sink: snapshots each presented (bytes, dest) and parks the completion
// so the test drives the serial drain by hand (proving exactly-one-outstanding).
struct recorder
{
    struct sent
    {
        std::vector<std::byte> bytes;
        int                    dest;
    };

    std::vector<sent>                   calls;
    std::vector<send_queue::completion> pending;

    send_queue::send_sink sink()
    {
        return [this](std::span<const std::byte> bytes, const int &dest,
                      send_queue::completion done)
        {
            calls.push_back(sent{std::vector<std::byte>(bytes.begin(), bytes.end()), dest});
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

TEST_CASE("send_queue copies caller bytes into an owned node on enqueue", "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink()};

    auto scratch = bytes_of({1, 2, 3});
    q.enqueue(scratch, 7);

    // Mutate the caller's scratch AFTER enqueue: the node must hold its own copy, so
    // the sink's captured bytes are unaffected (the non-owning-buffer hazard closed).
    scratch[0] = static_cast<std::byte>(99);

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0].dest == 7);
    REQUIRE(rec.calls[0].bytes == bytes_of({1, 2, 3}));
}

TEST_CASE("send_queue keeps at most one send-sink invocation outstanding", "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}), 1);
    q.enqueue(bytes_of({2}), 2);
    q.enqueue(bytes_of({3}), 3);

    // Only the front is in flight until its completion fires.
    REQUIRE(rec.calls.size() == 1);
    REQUIRE(q.sending());

    rec.complete_front();
    REQUIRE(rec.calls.size() == 2);

    rec.complete_front();
    REQUIRE(rec.calls.size() == 3);
}

TEST_CASE("send_queue drains in FIFO order across a burst", "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink()};

    q.enqueue(bytes_of({10}), 100);
    q.enqueue(bytes_of({20}), 200);
    q.enqueue(bytes_of({30}), 300);

    rec.complete_front();
    rec.complete_front();

    REQUIRE(rec.calls.size() == 3);
    REQUIRE(rec.calls[0].dest == 100);
    REQUIRE(rec.calls[1].dest == 200);
    REQUIRE(rec.calls[2].dest == 300);

    // The drain has emptied the queue; the block is idle.
    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("send_queue close() clears a pending queue and guards a late completion",
          "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}), 1);
    q.enqueue(bytes_of({2}), 2);
    REQUIRE(q.size() == 2);

    q.close();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());

    // A completion arriving after close is a guarded no-op: it must not chain or pop.
    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE(rec.calls.size() == 1); // no further sink invocation chained
}

TEST_CASE("send_queue default capacity is unbounded — the at-capacity signal is inert",
          "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink()}; // default capacity

    REQUIRE(q.full() == false);
    for(int i = 0; i < 1000; ++i)
        REQUIRE(q.enqueue(bytes_of({i & 0xff}), i));
    REQUIRE(q.full() == false);
    REQUIRE(q.size() == 1000);
}

TEST_CASE("send_queue bounded capacity fires the at-capacity signal and refuses past the cap",
          "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink(), 2}; // finite BYTE budget of two (single-byte frames)

    REQUIRE(q.enqueue(bytes_of({1}), 1)); // admitted, now in flight (1 byte)
    REQUIRE(q.enqueue(bytes_of({2}), 2)); // admitted, parked behind the front (2 bytes)
    REQUIRE(q.full());

    // At capacity: a further enqueue is refused (the backpressure signal), no node added.
    REQUIRE_FALSE(q.enqueue(bytes_of({3}), 3));
    REQUIRE(q.size() == 2);

    // A drain frees one slot; admission resumes.
    rec.complete_front();
    REQUIRE(q.size() == 1);
    REQUIRE_FALSE(q.full());
    REQUIRE(q.enqueue(bytes_of({3}), 3));
    REQUIRE(q.full());
    REQUIRE(q.size() == 2);
}

TEST_CASE("send_queue caps on summed BYTES, not entry count — one large frame trips a byte budget",
          "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink(), 8}; // an 8-byte budget

    // A single 5-byte frame is one ENTRY (count==1) but consumes 5 of 8 bytes.
    REQUIRE(q.enqueue(bytes_of({1, 2, 3, 4, 5}), 1));
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 5);
    REQUIRE_FALSE(q.full()); // count==1, but bytes (5) < cap (8)

    // A 3-byte frame fits exactly to the byte cap (5 + 3 == 8).
    REQUIRE(q.enqueue(bytes_of({6, 7, 8}), 2));
    REQUIRE(q.queued_bytes() == 8);
    REQUIRE(q.full()); // byte budget reached at count==2

    // A further 1-byte frame is refused on BYTES even though count is small.
    REQUIRE_FALSE(q.enqueue(bytes_of({9}), 3));
    REQUIRE(q.size() == 2);
    REQUIRE(q.queued_bytes() == 8);
}

TEST_CASE("send_queue near-cap boundary: byte accounting does not wrap and refuses correctly",
          "[io][send_queue]")
{
    // W1 overflow boundary: a frame at cap-1 bytes followed by a small frame whose sum
    // exceeds the cap must be refused (compare-before-add), and the running total must NOT
    // wrap below the cap and re-admit. Cap = 16; first frame = 15 bytes (cap-1).
    recorder   rec;
    send_queue q{rec.sink(), 16};

    REQUIRE(q.enqueue(std::vector<std::byte>(15), 1)); // 15 of 16 bytes
    REQUIRE(q.queued_bytes() == 15);
    REQUIRE_FALSE(q.full()); // one byte of headroom remains

    // A 2-byte frame would carry the total to 17 > 16 — refused, no wrap, nothing admitted.
    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(2), 2));
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 15); // unchanged — did NOT wrap or admit

    // The remaining one byte still admits exactly (15 + 1 == 16).
    REQUIRE(q.enqueue(std::vector<std::byte>(1), 3));
    REQUIRE(q.queued_bytes() == 16);
    REQUIRE(q.full());

    // A subsequent two-frame sum that would exceed the cap (a huge frame) is refused with
    // no overflow even at an extreme size near SIZE_MAX-class arithmetic boundaries.
    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(1), 4));
    REQUIRE(q.queued_bytes() == 16);
}
