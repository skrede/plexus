#include "test_stream_send_queue_common.h"

using namespace stream_send_queue_fixture;

TEST_CASE("stream_send_queue fail-on-error edge: a composer that closes the block stops the chain",
          "[io][stream_send_queue]")
{
    // The stream channel fails the channel on a socket error: it closes the block BEFORE
    // the completion would chain, so the post-close completion is a guarded no-op (the
    // next queued frame is NOT written through the failed socket). This pins the
    // fail-before-chain semantics — the error is surfaced to the composer, never swallowed.
    recorder          rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1}));
    q.enqueue(bytes_of({2}));
    REQUIRE(rec.calls.size() == 1); // only the front is in flight

    // Simulate the composer's fail path: on the error completion it closes the block.
    q.close();
    rec.complete_front(/*ok=*/false); // the failed write's completion fires post-close

    REQUIRE(rec.calls.size() == 1); // the second frame was NOT chained onto the failed socket
    REQUIRE(q.size() == 0);
}

namespace {

// A wire_bytes whose owner is a heap vector the test can observe: the owner is the
// std::shared_ptr keeping the bytes alive. use_count() witnesses that the block holds a
// reference to the owner across the in-flight write (the owner-lifetime invariant), and
// the view's data() witnesses no intermediate copy (the queue passes the owner's bytes).
plexus::wire_bytes<> owned_frame(std::initializer_list<int>               vals,
                                 std::shared_ptr<std::vector<std::byte>> &keep)
{
    auto buf = std::make_shared<std::vector<std::byte>>();
    for(int v : vals)
        buf->push_back(static_cast<std::byte>(v));
    keep = buf;
    return plexus::wire_bytes<>{std::span<const std::byte>{*buf}, buf};
}

}

TEST_CASE("stream_send_queue gathers a multi-frame burst into ONE drain turn over N views",
          "[io][stream_send_queue]")
{
    // The gather-write: a drain turn with N>1 queued frames issues exactly ONE sink call
    // carrying a SEQUENCE of N views, not N separate calls. The first frame drives
    // alone; the rest gather into the next turn behind it.
    recorder          rec;
    stream_send_queue q{rec.sink()};

    q.enqueue(bytes_of({1})); // turn 0, in flight alone
    q.enqueue(bytes_of({2}));
    q.enqueue(bytes_of({3}));
    q.enqueue(bytes_of({4})); // {2,3,4} park behind the in-flight turn

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0].size() == 1);

    rec.complete_front(); // turn 1 gathers {2,3,4} into ONE call
    REQUIRE(rec.calls.size() == 2);
    REQUIRE(rec.calls[1].size() == 3); // exactly one sink call for three frames
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
    recorder          rec;
    stream_send_queue q{rec.sink(), stream_send_queue::unbounded, /*gather_limit=*/3};

    for(int i = 0; i < 7; ++i)
        q.enqueue(bytes_of({i})); // {0} in flight alone, {1..6} parked

    REQUIRE(rec.calls.size() == 1);
    REQUIRE(rec.calls[0].size() == 1);

    rec.complete_front(); // turn 1: gathers {1,2,3} (capped at 3)
    REQUIRE(rec.calls[1].size() == 3);

    rec.complete_front(); // turn 2: gathers {4,5,6}
    REQUIRE(rec.calls[2].size() == 3);

    rec.complete_front();
    REQUIRE(q.size() == 0);
    REQUIRE_FALSE(q.sending());
}

TEST_CASE("stream_send_queue holds a wire_bytes owner with no copy and keeps it alive across the "
          "completion",
          "[io][stream_send_queue]")
{
    // The owner-handle path: enqueue(wire_bytes) holds the supplied owner and
    // passes its VIEW (no intermediate copy — the presented pointer equals the owner's
    // bytes), and the owner stays referenced by the block until the SINGLE completion
    // fires (releasing it before would be a read-after-free under a real gather-write).
    recorder          rec;
    stream_send_queue q{rec.sink()};

    std::shared_ptr<std::vector<std::byte>> keep;
    auto                                    frame       = owned_frame({7, 8, 9}, keep);
    const std::byte                        *owner_bytes = frame.data();
    REQUIRE(keep.use_count() == 2); // the test's keep + the frame's owner

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
    REQUIRE_FALSE(weak.expired()); // the block keeps the owner alive in flight

    rec.complete_front();    // the single completion releases the owner
    REQUIRE(weak.expired()); // released only AFTER the completion
    REQUIRE(q.size() == 0);
}

TEST_CASE("stream_send_queue keeps ALL gathered owners alive until the single completion",
          "[io][stream_send_queue]")
{
    // The gather-write hazard: ONE completion for N frames, so ALL N owners must outlive
    // it. Gather three owner frames into one turn, drop every external reference, and
    // assert none is freed until the single completion fires.
    recorder          rec;
    stream_send_queue q{rec.sink()};

    // A first frame drives alone; three owner frames then gather into the next turn.
    q.enqueue(bytes_of({0}));

    std::shared_ptr<std::vector<std::byte>> k1, k2, k3;
    auto                                    f1 = owned_frame({1}, k1);
    auto                                    f2 = owned_frame({2}, k2);
    auto                                    f3 = owned_frame({3}, k3);
    std::weak_ptr<std::vector<std::byte>>   w1{k1}, w2{k2}, w3{k3};

    q.enqueue(std::move(f1));
    q.enqueue(std::move(f2));
    q.enqueue(std::move(f3));
    k1.reset();
    k2.reset();
    k3.reset(); // drop every external reference

    rec.complete_front(); // turn 0 done; the three gather into turn 1
    REQUIRE(rec.calls[1].size() == 3);
    REQUIRE_FALSE(w1.expired()); // all three owners alive in flight
    REQUIRE_FALSE(w2.expired());
    REQUIRE_FALSE(w3.expired());

    rec.complete_front(); // the single completion releases all three
    REQUIRE(w1.expired());
    REQUIRE(w2.expired());
    REQUIRE(w3.expired());
    REQUIRE(q.size() == 0);
}
