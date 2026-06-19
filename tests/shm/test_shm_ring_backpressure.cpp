#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/slot_subscriber.h"
#include "plexus/io/shm/taken_message.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/ring_layout.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

// Backpressure proofs over an anonymous mapped span: best_effort overwrite skips
// a pinned slot (a held take is not stomped); a full-lap-pinned ring returns
// congested; reliable claim gates on the slowest registered cursor (lossless).

using namespace plexus::io::shm;

namespace {

struct backing_region
{
    explicit backing_region(std::size_t bytes)
            : m_storage(bytes + k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + (k_cache_line - 1)) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data       = reinterpret_cast<std::byte *>(aligned);
        m_size       = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte             *m_data{nullptr};
    std::size_t            m_size{0};
};

struct fixture
{
    static constexpr std::uint64_t k_cells = 16;
    static constexpr std::uint64_t k_slot  = 64;

    backing_region control{control_region_bytes(k_cells)};
    backing_region slab{slab_region_bytes(k_cells, k_slot)};
    broadcast_ring ring;

    fixture()
    {
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) ==
                loan_status::ok);
    }

    void put(plexus::io::reliability rel, std::uint32_t value)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(value), rel, plexus::io::congestion::drop_newest,
                                       claim) == loan_status::ok);
        std::memcpy(claim.slab.data(), &value, sizeof(value));
        REQUIRE(ring.commit(claim.position, sizeof(value)) == loan_status::ok);
    }
};

}

TEST_CASE("ring_backpressure: best_effort overwrite skips a pinned slot",
          "[shm][ring_backpressure]")
{
    fixture       f;
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
    fixture       f;
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
    REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::best_effort,
                                     plexus::io::congestion::drop_newest,
                                     claim) == loan_status::congested);

    // Unpin the lap.
    for(std::uint64_t i = 0; i < fixture::k_cells; ++i)
        f.ring.refcount_at(i).fetch_sub(1);

    f.ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_backpressure: a full-ring lap-behind reports lagged carrying the tail",
          "[shm][ring_backpressure]")
{
    fixture       f;
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

TEST_CASE("ring_backpressure: a skip tombstone at the cursor stays congested (one-slot step)",
          "[shm][ring_backpressure]")
{
    fixture       f;
    std::uint32_t cursor_index = 0;
    REQUIRE(f.ring.register_cursor(cursor_index) == loan_status::ok);
    const std::uint64_t pinned_pos = f.ring.tail_position();

    // Commit one value into the slot and pin it (a live take), so a producer that
    // laps back to this slot cannot recycle it and must donate a skip tombstone.
    f.put(plexus::io::reliability::best_effort, 0xABABABABu);
    REQUIRE(f.ring.pin_if_current(pinned_pos) == true);

    // Drive exactly one full lap of best_effort publishes. The producer laps back to
    // the pinned slot's position (pinned_pos + k_cells), finds it pinned, and stamps
    // a skip tombstone THERE (sequence = pinned_pos + k_cells + 1, len == k_skip_len).
    for(std::uint64_t i = 0; i < fixture::k_cells; ++i)
        f.put(plexus::io::reliability::best_effort, 0xCDCD0000u | static_cast<std::uint32_t>(i));

    // A consumer cursor exactly at the tombstoned position sees dif==0 &&
    // payload_len==k_skip_len -> congested (the one-slot step), NOT lagged.
    const std::uint64_t            tomb_cursor = pinned_pos + fixture::k_cells;
    broadcast_ring::consume_result tomb;
    REQUIRE(f.ring.consume(tomb_cursor, tomb) == loan_status::congested);

    f.ring.refcount_at(pinned_pos).fetch_sub(1);
    f.ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_backpressure: a lapped subscriber recovers in one take (O(1) jump)",
          "[shm][ring_backpressure]")
{
    fixture         f;
    slot_subscriber sub(f.ring);
    REQUIRE(sub.registered());
    const std::uint64_t start_cursor = sub.cursor();

    // Lap the subscriber's cursor by many full rings of best_effort publishes; the
    // subscriber has not called take(), so its cursor is stuck at the join tail.
    constexpr std::uint64_t k_laps = 5;
    for(std::uint64_t i = 0; i < fixture::k_cells * k_laps; ++i)
        f.put(plexus::io::reliability::best_effort, 0x88880000u | static_cast<std::uint32_t>(i));

    const std::uint64_t tail = f.ring.tail_position();
    REQUIRE(tail >= start_cursor + fixture::k_cells * k_laps);

    // ONE take() resolves the lag: the lagged status jumps the cursor straight to
    // the producer tail in a single step (O(1)), not one slot per call (O(depth)).
    // From the tail the ring is empty, so this single take() reports empty with the
    // cursor already at the tail -- the full lap distance closed in one call.
    taken_message msg;
    REQUIRE(sub.take(msg) == loan_status::empty);
    REQUIRE(sub.cursor() == tail);
    REQUIRE(sub.cursor() - start_cursor >= fixture::k_cells * k_laps);

    // A subsequent publish is now delivered in order from the jumped cursor: the
    // recovery landed the consumer on the live tail, not into a stale lap.
    f.put(plexus::io::reliability::best_effort, 0x99999999u);
    REQUIRE(sub.take(msg) == loan_status::ok);
    std::uint32_t read = 0;
    std::memcpy(&read, msg.bytes().data(), sizeof(read));
    REQUIRE(read == 0x99999999u);
}

TEST_CASE("ring_backpressure: reliable gates on the slowest cursor (lossless)",
          "[shm][ring_backpressure]")
{
    fixture       f;
    std::uint32_t cursor_index = 0;
    REQUIRE(f.ring.register_cursor(cursor_index) == loan_status::ok);
    std::uint64_t cursor = f.ring.tail_position();
    f.ring.publish_cursor(cursor_index, cursor);

    // Fill the whole ring under reliable; the consumer has not advanced, so once
    // the producer laps it must block (return congested) rather than overwrite an
    // unconsumed value -- lossless.
    for(std::uint64_t i = 0; i < fixture::k_cells; ++i)
        f.put(plexus::io::reliability::reliable, 0x44440000u | static_cast<std::uint32_t>(i));

    broadcast_ring::claim_result claim;
    REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                     plexus::io::congestion::block,
                                     claim) == loan_status::congested);

    // Consume every value in order: each must read back exactly what was written
    // (no value dropped), proving the reliable gate preserved the full sequence.
    for(std::uint64_t i = 0; i < fixture::k_cells; ++i)
    {
        broadcast_ring::consume_result consumed;
        REQUIRE(f.ring.consume(cursor, consumed) == loan_status::ok);
        std::uint32_t read = 0;
        std::memcpy(&read, consumed.slab.data(), sizeof(read));
        REQUIRE(read == (0x44440000u | static_cast<std::uint32_t>(i)));
        ++cursor;
        f.ring.publish_cursor(cursor_index, cursor);
    }

    // The cursor has now advanced past the prior lap: a reliable claim succeeds.
    REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                     plexus::io::congestion::block, claim) == loan_status::ok);

    f.ring.unregister_cursor(cursor_index);
}
