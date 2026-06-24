// The udp_backpressure_queue block oracle: a pure sans-IO drive of the bounded
// congestion=block parking queue (no socket, no ARQ). It proves the byte-accounting
// admission discipline the reliable channel relies on — admit-into-an-owned-slot,
// summed-payload-byte cap (NOT entry count), FIFO front/pop, and the near-cap overflow
// boundary (compare-before-add so a crafted large-frame sequence cannot wrap the running
// total below the cap and re-admit unboundedly, mitigating the integer-overflow
// threat). plexus::plexus only (header-only core; no backend link).

#include "plexus/datagram/detail/send_queue.h"
#include "plexus/datagram/detail/udp_backpressure_queue.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <utility>

using queue = plexus::datagram::detail::udp_backpressure_queue;

namespace {

std::vector<std::byte> frame(std::size_t n)
{
    return std::vector<std::byte>(n, std::byte{0xAB});
}

}

TEST_CASE("udp_backpressure_queue admits, accounts summed bytes, and pops FIFO", "[io][backpressure]")
{
    queue q{16}; // a 16-byte budget

    REQUIRE(q.empty());
    REQUIRE(q.queued_bytes() == 0);

    REQUIRE(q.admit(frame(5), /*fragmented=*/true));  // 5 bytes
    REQUIRE(q.admit(frame(7), /*fragmented=*/false)); // 12 bytes total
    REQUIRE(q.size() == 2);
    REQUIRE(q.queued_bytes() == 12);

    // Front is the first admitted; popping it frees its bytes. Each entry's FRAGMENTED
    // disposition rides FIFO with its bytes (the flag the reliable fragment path depends on).
    REQUIRE(q.front().size() == 5);
    REQUIRE(q.front_fragmented());
    q.pop_front();
    REQUIRE(q.queued_bytes() == 7);
    REQUIRE(q.front().size() == 7);
    REQUIRE_FALSE(q.front_fragmented());
}

TEST_CASE("udp_backpressure_queue caps on summed BYTES, not entry count", "[io][backpressure]")
{
    queue q{10};

    // One 9-byte frame is a single entry but consumes nearly the whole budget.
    REQUIRE(q.admit(frame(9), false));
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 9);

    // A 2-byte frame would carry the total to 11 > 10 — refused on BYTES at count==1.
    REQUIRE_FALSE(q.admit(frame(2), false));
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 9);

    // The remaining one byte admits exactly to the cap.
    REQUIRE(q.admit(frame(1), false));
    REQUIRE(q.queued_bytes() == 10);
}

TEST_CASE("udp_backpressure_queue near-cap boundary: byte accounting does not wrap", "[io][backpressure]")
{
    // overflow boundary: a frame at cap-1 bytes followed by a small frame
    // whose sum exceeds the cap is refused (compare-before-add), and the running total
    // does NOT wrap below the cap and re-admit. Cap = 32; first frame = 31 bytes (cap-1).
    queue q{32};

    REQUIRE(q.admit(frame(31), false)); // 31 of 32 bytes
    REQUIRE(q.queued_bytes() == 31);

    // A 2-byte frame would carry the total to 33 > 32 — refused, no wrap, nothing admitted.
    REQUIRE_FALSE(q.admit(frame(2), false));
    REQUIRE(q.size() == 1);
    REQUIRE(q.queued_bytes() == 31); // unchanged — did not wrap or admit

    // The remaining one byte still admits exactly (31 + 1 == 32).
    REQUIRE(q.admit(frame(1), false));
    REQUIRE(q.queued_bytes() == 32);

    // A subsequent admit at the cap is refused with no overflow.
    REQUIRE_FALSE(q.admit(frame(1), false));
    REQUIRE(q.queued_bytes() == 32);

    // Draining restores budget; admission resumes within the freed bytes.
    q.pop_front(); // frees 31 bytes
    REQUIRE(q.queued_bytes() == 1);
    REQUIRE(q.admit(frame(30), false));
    REQUIRE(q.queued_bytes() == 31);
}

TEST_CASE("send_queue: a finite byte cap refuses past the bound (the udp_server outbound bound)", "[io][backpressure][bound]")
{
    // The cap mechanism at the block the shared udp_server outbound queue is built from: a
    // finite byte_cap refuses enqueue past the cap (the at-capacity signal the server reacts
    // to under congestion), while an unbounded queue (today's server construction) admits
    // every datagram unboundedly. A WITHHOLDING sink (never invokes its completion)
    // simulates a stalled socket so the serial drain cannot free room.
    using sq = plexus::datagram::detail::send_queue<int>;

    // The bounded queue: a 16-byte cap, a sink that withholds completion so nothing drains.
    sq bounded{[](std::span<const std::byte>, const int &, sq::completion) { /* withhold */ }, 16};
    REQUIRE(bounded.enqueue(frame(10), 0));      // 10 of 16 — admitted, in flight (no completion)
    REQUIRE(bounded.enqueue(frame(6), 0));       // exactly to the cap — admitted into the queue
    REQUIRE_FALSE(bounded.enqueue(frame(1), 0)); // past the cap — REFUSED (the congestion signal)
    REQUIRE(bounded.queued_bytes() == 16);

    // The unbounded queue (today's server): the same withholding sink, no cap — it admits
    // far past any bound, the OOM path the finite cap closes.
    sq unbounded{[](std::span<const std::byte>, const int &, sq::completion) { /* withhold */ }};
    for(int i = 0; i < 1000; ++i)
        REQUIRE(unbounded.enqueue(frame(64), 0));
    REQUIRE(unbounded.queued_bytes() == 64 * 1000);
}
