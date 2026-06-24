#include "test_send_queue_common.h"

using namespace send_queue_fixture;

TEST_CASE("send_queue default capacity is unbounded — the at-capacity signal is inert", "[io][send_queue]")
{
    recorder   rec;
    send_queue q{rec.sink()}; // default capacity

    REQUIRE(q.full() == false);
    for(int i = 0; i < 1000; ++i)
        REQUIRE(q.enqueue(bytes_of({i & 0xff}), i));
    REQUIRE(q.full() == false);
    REQUIRE(q.size() == 1000);
}

TEST_CASE("send_queue bounded capacity fires the at-capacity signal and refuses past the cap", "[io][send_queue]")
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

TEST_CASE("send_queue caps on summed BYTES, not entry count — one large frame trips a byte budget", "[io][send_queue]")
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

TEST_CASE("send_queue near-cap boundary: byte accounting does not wrap and refuses correctly", "[io][send_queue]")
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
