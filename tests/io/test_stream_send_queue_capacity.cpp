#include "test_stream_send_queue_common.h"

using namespace stream_send_queue_fixture;

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

    REQUIRE(q.enqueue(bytes_of({1}))); // admitted, now in flight (1 byte)
    REQUIRE(q.enqueue(bytes_of({2}))); // admitted, parked behind the front (2 bytes)
    REQUIRE(q.full());

    REQUIRE_FALSE(q.enqueue(bytes_of({3}))); // refused at capacity, no node added
    REQUIRE(q.size() == 2);

    rec.complete_front(); // a drain frees one slot; admission resumes
    REQUIRE(q.size() == 1);
    REQUIRE_FALSE(q.full());
    REQUIRE(q.enqueue(bytes_of({3})));
    REQUIRE(q.full());
    REQUIRE(q.size() == 2);
}

TEST_CASE("stream_send_queue admits a single frame LARGER than the cap onto an empty queue", "[io][stream_send_queue]")
{
    // The decoupling contract: the byte cap bounds ADDITIONAL backlog, NOT a single message's
    // size — the per-message ceiling (enforced upstream at publish) is the sole size authority.
    // So an EMPTY queue must admit one frame of any size, even one far past the cap; only the
    // EXTRA frames queued behind it are refused once the cap is reached.
    recorder rec;
    stream_send_queue q{rec.sink(), 4}; // a tiny 4-byte backlog cap

    // A 10-byte frame (> the 4-byte cap) is admitted onto the empty queue and driven.
    REQUIRE(q.enqueue(std::vector<std::byte>(10)));
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 10);
    REQUIRE(q.full()); // already past the cap: no more backlog

    // With the cap already exceeded by the in-flight frame, a further frame is refused —
    // the cap still bounds the EXTRA backlog behind the admitted message.
    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(1)));
    REQUIRE(q.size() == 1);

    // Once the oversized frame drains, the empty queue again admits one frame of any size.
    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.full());
    REQUIRE(q.enqueue(std::vector<std::byte>(9))); // another > cap frame onto the now-empty queue
    REQUIRE(q.queued_bytes() == 9);
}

TEST_CASE("stream_send_queue caps on summed BYTES, not entry count", "[io][stream_send_queue]")
{
    recorder rec;
    stream_send_queue q{rec.sink(), 8};

    REQUIRE(q.enqueue(bytes_of({1, 2, 3, 4, 5}))); // one ENTRY, 5 of 8 bytes
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 5);
    REQUIRE_FALSE(q.full());

    REQUIRE(q.enqueue(bytes_of({6, 7, 8}))); // exactly to the byte cap (5 + 3 == 8)
    REQUIRE(q.queued_bytes() == 8);
    REQUIRE(q.full());

    REQUIRE_FALSE(q.enqueue(bytes_of({9}))); // refused on BYTES, count small
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

    REQUIRE(q.enqueue(std::vector<std::byte>(15))); // 15 of 16 bytes
    REQUIRE(q.queued_bytes() == 15);
    REQUIRE_FALSE(q.full());

    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(2))); // 17 > 16 — refused, no wrap
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 15);

    REQUIRE(q.enqueue(std::vector<std::byte>(1))); // the remaining byte fits exactly
    REQUIRE(q.queued_bytes() == 16);
    REQUIRE(q.full());

    REQUIRE_FALSE(q.enqueue(std::vector<std::byte>(1)));
    REQUIRE(q.queued_bytes() == 16);
}
