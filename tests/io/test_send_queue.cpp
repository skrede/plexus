#include "test_send_queue_common.h"

using namespace send_queue_fixture;

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
