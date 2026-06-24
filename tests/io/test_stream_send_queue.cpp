#include "test_stream_send_queue_common.h"

using namespace stream_send_queue_fixture;

TEST_CASE("stream_send_queue copies caller bytes into an owned node on enqueue", "[io][stream_send_queue]")
{
    recorder          rec;
    stream_send_queue q{rec.sink()};

    auto scratch = bytes_of({1, 2, 3});
    q.enqueue(scratch);

    // Mutate the caller's scratch AFTER enqueue: the node must hold its own copy.
    scratch[0] = static_cast<std::byte>(99);

    REQUIRE(rec.calls.size() == 1);    // one drain turn
    REQUIRE(rec.calls[0].size() == 1); // one gathered node
    REQUIRE(rec.calls[0][0] == bytes_of({1, 2, 3}));
}

TEST_CASE("stream_send_queue keeps at most one send-sink invocation outstanding", "[io][stream_send_queue]")
{
    // The first enqueue drives immediately and alone; the next two park behind the
    // in-flight turn and gather into the NEXT turn. At most ONE turn is outstanding at a
    // time (the serial discipline), and the parked frames coalesce into a single call.
    recorder          rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1})); // turn 0: in flight alone
    q.enqueue(bytes_of({2}));
    q.enqueue(bytes_of({3})); // {2,3} parked

    REQUIRE(rec.calls.size() == 1); // one turn outstanding
    REQUIRE(rec.calls[0].size() == 1);
    REQUIRE(q.sending());

    rec.complete_front(); // turn 1 gathers {2,3} into ONE call
    REQUIRE(rec.calls.size() == 2);
    REQUIRE(rec.calls[1].size() == 2);

    rec.complete_front();
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue drains in FIFO order across a burst", "[io][stream_send_queue]")
{
    // Enqueue while a turn is in flight: the in-flight front gathers alone, the later
    // frames gather into the NEXT turn in FIFO order behind it.
    recorder          rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({10})); // turn 0: in flight alone
    q.enqueue(bytes_of({20})); // parked behind the in-flight turn
    q.enqueue(bytes_of({30}));

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0] == std::vector<std::vector<std::byte>>{bytes_of({10})});

    rec.complete_front(); // turn 0 done; turn 1 gathers {20, 30}
    REQUIRE(rec.calls.size() == 2);
    REQUIRE(rec.calls[1] == std::vector<std::vector<std::byte>>{bytes_of({20}), bytes_of({30})});

    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue close() clears a pending queue and guards a late completion", "[io][stream_send_queue]")
{
    recorder          rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1})); // gathers into the in-flight turn
    q.enqueue(bytes_of({2})); // rides the same gather turn
    REQUIRE(q.size() == 2);

    q.close();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());

    // A completion arriving after close is a guarded no-op: it must not chain or pop.
    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE(rec.calls.size() == 1);
}
