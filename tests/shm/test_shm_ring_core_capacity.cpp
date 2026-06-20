#include "test_shm_ring_core_common.h"

using namespace shm_ring_core_fixture;

TEST_CASE("ring_core: the reliable-reclaim scan is bounded by the registered high-water",
          "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot  = 64;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) ==
            loan_status::ok);

    // A fresh ring scans nothing (no registered consumer gates reclamation).
    REQUIRE(ring.registered_high_water() == 0);

    // Two registered cursors raise the high-water to exactly 2 (dense ascending
    // allocation), so the reclaim scan touches 2 cursor slots, not k_max_consumers.
    std::uint32_t a = 0;
    std::uint32_t b = 0;
    REQUIRE(ring.register_cursor(a) == loan_status::ok);
    REQUIRE(ring.register_cursor(b) == loan_status::ok);
    REQUIRE(a == 0);
    REQUIRE(b == 1);
    REQUIRE(ring.registered_high_water() == 2);
    REQUIRE(ring.registered_high_water() < k_max_consumers);

    // Correctness within the bound: the reliable gate still holds on the slowest of
    // the two registered cursors. Park cursor `a` at the join tail (it gates) and
    // advance `b` past a full lap; once the producer laps, the claim must congest on
    // `a` -- proving the bounded scan still reads the gating cursor, never skips it.
    std::uint64_t cur_a = ring.tail_position();
    std::uint64_t cur_b = ring.tail_position();
    ring.publish_cursor(a, cur_a);
    ring.publish_cursor(b, cur_b);

    for(std::uint64_t i = 0; i < k_cells; ++i)
    {
        broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                       plexus::io::congestion::block, claim) == loan_status::ok);
        const std::uint32_t value = 0xBEEF0000u | static_cast<std::uint32_t>(i);
        std::memcpy(claim.slab.data(), &value, sizeof(value));
        REQUIRE(ring.commit(claim.position, sizeof(value)) == loan_status::ok);

        // Drain on `b` only; `a` stays parked at the tail and must keep gating.
        broadcast_ring::consume_result consumed;
        REQUIRE(ring.consume(cur_b, consumed) == loan_status::ok);
        ++cur_b;
        ring.publish_cursor(b, cur_b);
    }

    // The ring is full and `a` has consumed nothing: the reliable claim must congest
    // on `a` (the slowest registered cursor) -- the bounded scan read it.
    broadcast_ring::claim_result blocked;
    REQUIRE(ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable,
                                   plexus::io::congestion::block,
                                   blocked) == loan_status::congested);

    // Unregistering does not shrink the high-water (monotonic, == the prior
    // full-scan worst case, never a regression).
    ring.unregister_cursor(b);
    REQUIRE(ring.registered_high_water() == 2);

    ring.unregister_cursor(a);
}

TEST_CASE("ring_core: attach re-reads and bounds-checks the header", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 32;
    constexpr std::uint64_t k_slot  = 128;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring creator;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, creator) ==
            loan_status::ok);

    // A second ring attaches the SAME backing spans and re-reads the geometry.
    broadcast_ring attacher;
    REQUIRE(broadcast_ring::attach(control.span(), slab.span(), attacher) == loan_status::ok);
    REQUIRE(attacher.cell_count() == k_cells);
    REQUIRE(attacher.slot_capacity() == k_slot);

    // A foreign / unmapped control region (no magic) is rejected.
    backing_region foreign_control(control_region_bytes(k_cells));
    broadcast_ring rejected;
    REQUIRE(broadcast_ring::attach(foreign_control.span(), slab.span(), rejected) ==
            loan_status::rejected);
}

TEST_CASE("ring_core: a per-ring consumer_capacity bounds cursor registration", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells    = 16;
    constexpr std::uint64_t k_slot     = 64;
    constexpr std::uint64_t k_capacity = 4;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring,
                                   k_capacity) == loan_status::ok);

    // The first C registrations succeed; the (C+1)th rejects below the absolute cap.
    std::uint32_t idx[k_capacity] = {};
    for(std::uint64_t i = 0; i < k_capacity; ++i)
        REQUIRE(ring.register_cursor(idx[i]) == loan_status::ok);
    std::uint32_t overflow = 0;
    REQUIRE(ring.register_cursor(overflow) == loan_status::rejected);
    REQUIRE(k_capacity < k_max_consumers);

    // publish/unregister operate within the capacity with no out-of-range touch.
    ring.publish_cursor(idx[0], ring.tail_position());
    for(std::uint64_t i = 0; i < k_capacity; ++i)
        ring.unregister_cursor(idx[i]);

    // An attacher re-reads the stamped capacity from the header.
    broadcast_ring attacher;
    REQUIRE(broadcast_ring::attach(control.span(), slab.span(), attacher) == loan_status::ok);
    std::uint32_t a_idx[k_capacity] = {};
    for(std::uint64_t i = 0; i < k_capacity; ++i)
        REQUIRE(attacher.register_cursor(a_idx[i]) == loan_status::ok);
    std::uint32_t a_overflow = 0;
    REQUIRE(attacher.register_cursor(a_overflow) == loan_status::rejected);
}

TEST_CASE("ring_core: create rejects an out-of-range consumer_capacity", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot  = 64;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring zero;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, zero, 0) ==
            loan_status::rejected);

    broadcast_ring over;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, over,
                                   k_max_consumers + 1) == loan_status::rejected);

    // The shipped default (omitted argument) resolves to the absolute cap and admits it.
    broadcast_ring def;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, def) ==
            loan_status::ok);
}
