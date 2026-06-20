#include "test_shm_ring_core_common.h"

using namespace shm_ring_core_fixture;

TEST_CASE("ring_sizing: ring_geometry_for trades depth for width under a ceiling",
          "[shm][ring_sizing]")
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
    for(std::uint32_t payload : {64u, 4096u, 40000u, 70000u, 131072u})
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
    for(auto [cap, depth] : {std::pair<std::uint32_t, std::uint64_t>{16u, 32u}, {2u, 4u}, {1u, 2u}})
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
    for(auto [cap, depth] : {std::pair<std::uint32_t, std::uint64_t>{16u, 16u}, {2u, 2u}, {4u, 4u}})
    {
        const ring_geometry g =
                ring_geometry_for(k_deep, ring_geometry_mode::best_effort_large, cap);
        REQUIRE(g.cell_count == depth);
    }

    // A capacity of 0 resolves to the shipped capacity floor (k_max_consumers).
    REQUIRE(ring_geometry_for(k_deep, ring_geometry_mode::reliable_preserving, 0u).cell_count ==
            ring_geometry_for(k_deep, ring_geometry_mode::reliable_preserving,
                              static_cast<std::uint32_t>(k_max_consumers))
                    .cell_count);
}

TEST_CASE("ring_sizing: ring_memory_for is the pure byte cost of the derived ring",
          "[shm][ring_sizing]")
{
    for(std::uint32_t payload : {64u, 40000u, 131072u})
        for(auto mode : {ring_geometry_mode::reliable_preserving,
                         ring_geometry_mode::best_effort_large, ring_geometry_mode::wire_fallback})
        {
            const ring_geometry g = ring_geometry_for(payload, mode, 8u);
            REQUIRE(ring_memory_for(payload, mode, 8u) ==
                    control_region_bytes(g.cell_count) +
                            slab_region_bytes(g.cell_count, g.slot_capacity));
        }
}

TEST_CASE("ring_layout: the cross-process structs are lock-free standard-layout",
          "[shm][ring_sizing]")
{
    STATIC_REQUIRE(std::atomic<std::uint64_t>::is_always_lock_free);
    STATIC_REQUIRE(std::atomic<std::uint32_t>::is_always_lock_free);
    STATIC_REQUIRE(std::is_standard_layout_v<cell_t>);
    STATIC_REQUIRE(std::is_standard_layout_v<cursor_t>);
    STATIC_REQUIRE(std::is_standard_layout_v<control_header_t>);
    REQUIRE(k_cache_line == 64);
}

TEST_CASE("ring_core: claim/commit/consume round-trips a payload single-process",
          "[shm][ring_core]")
{
    constexpr std::uint64_t k_cells      = 64;
    constexpr std::uint64_t k_slot       = 256;
    constexpr int           k_iterations = 200;

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
    for(int i = 0; i < k_iterations; ++i)
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
