#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/loan_status.h"

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
        auto base = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + (k_cache_line - 1)) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        m_data = reinterpret_cast<std::byte *>(aligned);
        m_size = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte *m_data{nullptr};
    std::size_t m_size{0};
};

struct fixture
{
    static constexpr std::uint64_t k_cells = 16;
    static constexpr std::uint64_t k_slot = 64;

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
        REQUIRE(ring.claim_with_policy(sizeof(value), rel, plexus::io::congestion::drop_newest, claim) == loan_status::ok);
        std::memcpy(claim.slab.data(), &value, sizeof(value));
        REQUIRE(ring.commit(claim.position, sizeof(value)) == loan_status::ok);
    }
};

}

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
    for (std::uint64_t i = 0; i < fixture::k_cells * 4; ++i)
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
    for (std::uint64_t i = 0; i < fixture::k_cells; ++i)
    {
        f.put(plexus::io::reliability::best_effort, 0x33330000u | static_cast<std::uint32_t>(i));
        REQUIRE(f.ring.pin_if_current(cursor) == true);
        ++cursor;
    }

    // Every cell is pinned: a best_effort claim can find no recyclable slot and
    // returns congested as the bounded fallback (it never stomps a live take).
    broadcast_ring::claim_result claim;
    REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::best_effort,
                                     plexus::io::congestion::drop_newest, claim) == loan_status::congested);

    // Unpin the lap.
    for (std::uint64_t i = 0; i < fixture::k_cells; ++i)
        f.ring.refcount_at(i).fetch_sub(1);

    f.ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_backpressure: reliable gates on the slowest cursor (lossless)", "[shm][ring_backpressure]")
{
    fixture f;
    std::uint32_t cursor_index = 0;
    REQUIRE(f.ring.register_cursor(cursor_index) == loan_status::ok);
    std::uint64_t cursor = f.ring.tail_position();
    f.ring.publish_cursor(cursor_index, cursor);

    // Fill the whole ring under reliable; the consumer has not advanced, so once
    // the producer laps it must block (return congested) rather than overwrite an
    // unconsumed value -- lossless.
    for (std::uint64_t i = 0; i < fixture::k_cells; ++i)
        f.put(plexus::io::reliability::reliable, 0x44440000u | static_cast<std::uint32_t>(i));

    broadcast_ring::claim_result claim;
    REQUIRE(f.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                     plexus::io::congestion::block, claim) == loan_status::congested);

    // Consume every value in order: each must read back exactly what was written
    // (no value dropped), proving the reliable gate preserved the full sequence.
    for (std::uint64_t i = 0; i < fixture::k_cells; ++i)
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
