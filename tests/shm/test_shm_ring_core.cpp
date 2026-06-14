#include "support/xproc_harness.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_geometry.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

// Single-process ring proofs over an anonymous mapped span. The ring takes a
// caller-supplied (control, slab) region pair, so a test maps two heap-backed
// spans and drives claim/commit/consume with no POSIX broker. ring_sizing
// exercises ring_geometry_for + the slab-ceiling fast-fail; ring_core round-trips
// a payload looped N>=100.

using namespace plexus::io::shm;

namespace {

// Heap-backed standin for a mapped region: a properly-aligned byte buffer the
// ring places its header/cells (control) or its payload slab over. A real
// backend maps /dev/shm; here a vector with cache-line-aligned storage suffices
// for the single-process logic proof.
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

}

TEST_CASE("ring_sizing: ring_geometry_for trades depth for width under a ceiling", "[shm][ring_sizing]")
{
    // Default deep-but-narrow ring for an unset / small declaration -- byte-identical
    // to the shipped default (reliable_preserving, capacity 16).
    const ring_geometry small = ring_geometry_for(std::nullopt);
    REQUIRE(small.cell_count == 256);
    REQUIRE(small.slot_capacity == 4096);
    REQUIRE(small.cell_count > k_max_consumers);

    // The mid-payload tiers step depth down as the slot grows while staying under the
    // ceiling and strictly above the declared capacity. (The deepest band's depth is
    // capacity-derived and is exercised below, where a large declaration can exceed
    // the ceiling on purpose -- that over-ceiling case is the caller's to detect.)
    constexpr std::uint64_t k_max_ring_slab_bytes = 16ull * 1024 * 1024;
    for (std::uint32_t payload : {64u, 4096u, 40000u, 70000u, 131072u})
    {
        const ring_geometry g = ring_geometry_for(payload);
        REQUIRE(g.slot_capacity >= payload);
        REQUIRE(g.slot_capacity % 8 == 0);
        REQUIRE((g.cell_count & (g.cell_count - 1)) == 0); // power of two
        REQUIRE(g.cell_count > k_max_consumers);
        REQUIRE(g.cell_count * g.slot_capacity <= k_max_ring_slab_bytes);
    }
}

TEST_CASE("ring_sizing: the deep band depth is capacity-derived per mode", "[shm][ring_sizing]")
{
    constexpr std::uint32_t k_deep = 200000u;

    // reliable_preserving: depth is the next power of two STRICTLY above capacity
    // (16 -> 32, 2 -> 4, 1 -> 2), never depth-17.
    for (auto [cap, depth] : {std::pair<std::uint32_t, std::uint64_t>{16u, 32u},
                              {2u, 4u}, {1u, 2u}})
    {
        const ring_geometry g =
            ring_geometry_for(k_deep, ring_geometry_mode::reliable_preserving, cap);
        REQUIRE(g.cell_count == depth);
        REQUIRE(g.cell_count > cap);
        REQUIRE(g.slot_capacity == round_up_8(k_deep));
    }

    // wire_fallback reuses the reliable derivation at this layer (the bounded ring is
    // a reliable ring; the reroute is not a geometry concern).
    REQUIRE(ring_geometry_for(k_deep, ring_geometry_mode::wire_fallback, 16u).cell_count ==
            ring_geometry_for(k_deep, ring_geometry_mode::reliable_preserving, 16u).cell_count);

    // best_effort_large admits depth == capacity (the low-memory deep band).
    for (auto [cap, depth] : {std::pair<std::uint32_t, std::uint64_t>{16u, 16u},
                              {2u, 2u}, {4u, 4u}})
    {
        const ring_geometry g =
            ring_geometry_for(k_deep, ring_geometry_mode::best_effort_large, cap);
        REQUIRE(g.cell_count == depth);
    }

    // A capacity of 0 resolves to the shipped capacity floor (k_max_consumers).
    REQUIRE(ring_geometry_for(k_deep, ring_geometry_mode::reliable_preserving, 0u).cell_count ==
            ring_geometry_for(k_deep, ring_geometry_mode::reliable_preserving,
                              static_cast<std::uint32_t>(k_max_consumers)).cell_count);
}

TEST_CASE("ring_sizing: ring_memory_for is the pure byte cost of the derived ring", "[shm][ring_sizing]")
{
    for (std::uint32_t payload : {64u, 40000u, 131072u})
        for (auto mode : {ring_geometry_mode::reliable_preserving,
                          ring_geometry_mode::best_effort_large,
                          ring_geometry_mode::wire_fallback})
        {
            const ring_geometry g = ring_geometry_for(payload, mode, 8u);
            REQUIRE(ring_memory_for(payload, mode, 8u) ==
                    control_region_bytes(g.cell_count) +
                        slab_region_bytes(g.cell_count, g.slot_capacity));
        }
}

TEST_CASE("ring_layout: the cross-process structs are lock-free standard-layout", "[shm][ring_sizing]")
{
    STATIC_REQUIRE(std::atomic<std::uint64_t>::is_always_lock_free);
    STATIC_REQUIRE(std::atomic<std::uint32_t>::is_always_lock_free);
    STATIC_REQUIRE(std::is_standard_layout_v<cell_t>);
    STATIC_REQUIRE(std::is_standard_layout_v<cursor_t>);
    STATIC_REQUIRE(std::is_standard_layout_v<control_header_t>);
    REQUIRE(k_cache_line == 64);
}

TEST_CASE("ring_core: claim/commit/consume round-trips a payload single-process", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 64;
    constexpr std::uint64_t k_slot = 256;
    constexpr int k_iterations = 200;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring) ==
            loan_status::ok);

    std::uint32_t cursor_index = 0;
    REQUIRE(ring.register_cursor(cursor_index) == loan_status::ok);
    std::uint64_t cursor = ring.tail_position();
    ring.publish_cursor(cursor_index, cursor);

    // Loop well past one lap (200 > 64 cells): the reliable claim gates on this
    // single registered cursor, which advances each iteration, so reclamation
    // keeps pace and the round-trip never congests. Each consumed value must read
    // back exactly the bytes the producer wrote.
    for (int i = 0; i < k_iterations; ++i)
    {
        const std::uint32_t value = 0xC0DE0000u | static_cast<std::uint32_t>(i);

        broadcast_ring::claim_result claim;
        REQUIRE(ring.claim_with_policy(sizeof(value), plexus::io::reliability::reliable,
                                       plexus::io::congestion::block, claim) == loan_status::ok);
        std::memcpy(claim.slab.data(), &value, sizeof(value));
        REQUIRE(ring.commit(claim.position, sizeof(value)) == loan_status::ok);

        broadcast_ring::consume_result consumed;
        REQUIRE(ring.consume(cursor, consumed) == loan_status::ok);
        REQUIRE(consumed.slab.size() == sizeof(value));

        std::uint32_t read = 0;
        std::memcpy(&read, consumed.slab.data(), sizeof(read));
        REQUIRE(read == value);

        ++cursor;
        ring.publish_cursor(cursor_index, cursor);
    }

    ring.unregister_cursor(cursor_index);
}

TEST_CASE("ring_core: the reliable-reclaim scan is bounded by the registered high-water", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot = 64;

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

    for (std::uint64_t i = 0; i < k_cells; ++i)
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
                                   plexus::io::congestion::block, blocked) == loan_status::congested);

    // Unregistering does not shrink the high-water (monotonic, == the prior
    // full-scan worst case, never a regression).
    ring.unregister_cursor(b);
    REQUIRE(ring.registered_high_water() == 2);

    ring.unregister_cursor(a);
}

TEST_CASE("ring_core: attach re-reads and bounds-checks the header", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 32;
    constexpr std::uint64_t k_slot = 128;

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
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot = 64;
    constexpr std::uint64_t k_capacity = 4;

    backing_region control(control_region_bytes(k_cells));
    backing_region slab(slab_region_bytes(k_cells, k_slot));

    broadcast_ring ring;
    REQUIRE(broadcast_ring::create(control.span(), slab.span(), k_cells, k_slot, ring, k_capacity) ==
            loan_status::ok);

    // The first C registrations succeed; the (C+1)th rejects below the absolute cap.
    std::uint32_t idx[k_capacity] = {};
    for (std::uint64_t i = 0; i < k_capacity; ++i)
        REQUIRE(ring.register_cursor(idx[i]) == loan_status::ok);
    std::uint32_t overflow = 0;
    REQUIRE(ring.register_cursor(overflow) == loan_status::rejected);
    REQUIRE(k_capacity < k_max_consumers);

    // publish/unregister operate within the capacity with no out-of-range touch.
    ring.publish_cursor(idx[0], ring.tail_position());
    for (std::uint64_t i = 0; i < k_capacity; ++i)
        ring.unregister_cursor(idx[i]);

    // An attacher re-reads the stamped capacity from the header.
    broadcast_ring attacher;
    REQUIRE(broadcast_ring::attach(control.span(), slab.span(), attacher) == loan_status::ok);
    std::uint32_t a_idx[k_capacity] = {};
    for (std::uint64_t i = 0; i < k_capacity; ++i)
        REQUIRE(attacher.register_cursor(a_idx[i]) == loan_status::ok);
    std::uint32_t a_overflow = 0;
    REQUIRE(attacher.register_cursor(a_overflow) == loan_status::rejected);
}

TEST_CASE("ring_core: create rejects an out-of-range consumer_capacity", "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells = 16;
    constexpr std::uint64_t k_slot = 64;

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
