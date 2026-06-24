#include "test_shm_ring_backpressure_common.h"

using namespace shm_ring_backpressure_fixture;

TEST_CASE("ring_backpressure: best_effort overwrite skips a pinned slot", "[shm][ring_backpressure]")
{
    fixture f;
    std::uint32_t cursor_index = 0;
    REQUIRE(f.ring.register_cursor(cursor_index) == loan_status::ok);
    std::uint64_t cursor = f.ring.tail_position();

    // Publish one value and pin it via the Dekker reader half (a held take()).
    f.put(plexus::io::reliability::best_effort, 0x11111111u);
    REQUIRE(f.ring.pin_if_current(cursor) == true);
    const std::uint64_t pinned_pos = cursor;

    // Drive a full lap of best_effort publishes. The overwrite must SKIP the
    // pinned cell (donating a tombstone) rather than stomping the held read.
    for(std::uint64_t i = 0; i < fixture::k_cells * 4; ++i)
        f.put(plexus::io::reliability::best_effort, 0x22220000u | static_cast<std::uint32_t>(i));

    // The pinned slot's refcount is still held (>0): the producer never recycled it.
    REQUIRE(f.ring.refcount_at(pinned_pos).load() > 0);

    // Release the pin.
    f.ring.refcount_at(pinned_pos).fetch_sub(1);
    REQUIRE(f.ring.refcount_at(pinned_pos).load() == 0);

    f.ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_backpressure: a full-lap-pinned ring returns congested", "[shm][ring_backpressure]")
{
    fixture f;
    std::uint32_t cursor_index = 0;
    REQUIRE(f.ring.register_cursor(cursor_index) == loan_status::ok);

    // Fill and pin an entire lap: publish k_cells values, pinning each committed
    // slot so every cell carries take_refcount > 0.
    std::uint64_t cursor = f.ring.tail_position();
    for(std::uint64_t i = 0; i < fixture::k_cells; ++i)
    {
        f.put(plexus::io::reliability::best_effort, 0x33330000u | static_cast<std::uint32_t>(i));
        REQUIRE(f.ring.pin_if_current(cursor) == true);
        ++cursor;
    }

    // Every cell is pinned: a best_effort claim can find no recyclable slot and
    // returns congested as the bounded fallback (it never stomps a live take).
    broadcast_ring::claim_result claim;
    REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest, claim) == loan_status::congested);

    // Unpin the lap.
    for(std::uint64_t i = 0; i < fixture::k_cells; ++i)
        f.ring.refcount_at(i).fetch_sub(1);

    f.ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_backpressure: a full-ring lap-behind reports lagged carrying the tail", "[shm][ring_backpressure]")
{
    fixture f;
    std::uint32_t cursor_index = 0;
    REQUIRE(f.ring.register_cursor(cursor_index) == loan_status::ok);
    const std::uint64_t start = f.ring.tail_position();

    // Lap the registered cursor by well over a full ring with best_effort publishes
    // (none pinned, so every cell recycles). The cursor still sits at `start`, now a
    // full ring or more behind the producer.
    for(std::uint64_t i = 0; i < fixture::k_cells * 3; ++i)
        f.put(plexus::io::reliability::best_effort, 0x55550000u | static_cast<std::uint32_t>(i));

    // consume() at the stale cursor detects the full-ring lap (dif >= cell_count)
    // and returns lagged carrying the producer tail to jump to -- NOT congested.
    broadcast_ring::consume_result consumed;
    REQUIRE(f.ring.consume(start, consumed) == loan_status::lagged);
    REQUIRE(consumed.position == f.ring.tail_position());
    REQUIRE(consumed.position >= start + fixture::k_cells);

    f.ring.unregister_cursor(cursor_index);
}
