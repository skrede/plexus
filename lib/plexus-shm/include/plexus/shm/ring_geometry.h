#ifndef HPP_GUARD_PLEXUS_SHM_RING_GEOMETRY_H
#define HPP_GUARD_PLEXUS_SHM_RING_GEOMETRY_H

#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/ring_layout.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::shm {

// The per-ring slab ceiling: the largest /dev/shm slab a single ring may size, so an
// oversize payload declaration cannot map an unbounded region. It is the layout-side
// bound (depth * round_up_8(payload)), independent of the decode layer's upstream
// max_payload cap. It is the SHIPPED default for the node_options slab-ceiling knob
// threaded to the registry; the live ceiling is the node value, with this constant the
// ultimate default. The registration path (not this constant) fails closed above it.
//
// Chosen at 512 MiB, justified from the depth-32 cost-per-ring table (slab dominates;
// control+cells adds ~3.4 KiB on a depth-32 ring): a depth-D ring at payload P costs
// 1344 + D*64 + D*round_up_8(P) bytes. 512 MiB is the smallest power-of-two-MiB ceiling
// that admits BOTH operator-locked rings — the 8 MiB default-config x depth-32 ring
// (256.0032 MiB, just over a bare 256 MiB because of the control overhead) AND the
// headline 16 MiB reliable_preserving round-trip at a typical (small) fan-out (C<=8 ->
// depth<=16 -> <=256.0023 MiB) — while leaving the full-16-consumer depth-32 16 MiB tail
// (512.0032 MiB, over by the control overhead) as the documented fail-closed corner. A
// bare 256 MiB would fail the default-config ring; 512 MiB clears it with the depth-32
// control overhead and stays generous without admitting the expensive full-fan-out tail.
constexpr std::uint64_t k_max_ring_slab_bytes = 512ull * 1024 * 1024;

// The depth + per-slot stride a broadcast ring is created with. cell_count is the
// power-of-two Vyukov cell depth; slot_capacity is the per-slot byte stride
// (always a multiple of eight so slot i at i*stride is 8-aligned).
struct ring_geometry
{
    std::uint64_t cell_count;
    std::uint64_t slot_capacity;
};

// Smallest power of two STRICTLY greater than v -- the reliable depth floor: a
// reliable ring needs depth > consumers so the slowest consumer always leaves the
// producer a free cell (the measured half-throughput stall at depth == consumers).
// There is no depth-17, so a 16-consumer reliable ring is depth 32, never 17.
[[nodiscard]] constexpr std::uint64_t next_pow2_strictly_above(std::uint64_t v) noexcept
{
    std::uint64_t p = 1;
    while(p <= v)
        p <<= 1;
    return p;
}

// Smallest power of two at least v -- the best-effort depth (depth == consumers is
// admissible, trading the reliable headroom for low memory).
[[nodiscard]] constexpr std::uint64_t next_pow2_at_least(std::uint64_t v) noexcept
{
    std::uint64_t p = 1;
    while(p < v)
        p <<= 1;
    return p;
}

// The depth/width tier selection: trade depth for width so per-ring memory stays bounded as
// the slot grows. Beyond the fixed-depth tiers the slot dominates, so the depth pins to the
// capacity floor (> capacity for reliable modes; best_effort_large admits depth == capacity).
[[nodiscard]] inline ring_geometry ring_geometry_tier(std::uint64_t want, std::uint64_t slot,
                                                      ring_geometry_mode mode,
                                                      std::uint64_t      capacity) noexcept
{
    if(want > 131072)
    {
        const std::uint64_t depth = mode == ring_geometry_mode::best_effort_large
                ? next_pow2_at_least(capacity)
                : next_pow2_strictly_above(capacity);
        return ring_geometry{depth, slot};
    }
    if(want > 65536)
        return ring_geometry{64, slot};
    if(want > 32768)
        return ring_geometry{128, slot};
    if(want > 4096)
        return ring_geometry{256, slot};
    return ring_geometry{256, 4096};
}

// Maps a topic's declared maximum payload, geometry mode, and per-ring consumer capacity to a
// ring geometry. cell_count is always a power of two and, under the reliable-geometry modes,
// strictly exceeds the declared capacity; best_effort_large admits depth == capacity. A pure
// layout query: it never bounds the slab itself — an oversize declaration that exceeds the
// per-ring ceiling is left for the caller to detect via ring_memory_for (the fail-closed
// registration path owns that bound + diagnostic).
[[nodiscard]] inline ring_geometry ring_geometry_for(std::optional<std::uint32_t> max_payload,
                                                     ring_geometry_mode           mode,
                                                     std::uint32_t consumer_capacity) noexcept
{
    const std::uint64_t capacity = consumer_capacity == 0 ? k_max_consumers : consumer_capacity;
    const std::uint64_t want     = max_payload.value_or(0);
    ring_geometry       geom     = ring_geometry_tier(want, round_up_8(want), mode, capacity);
    geom.slot_capacity           = round_up_8(geom.slot_capacity);
    if(mode != ring_geometry_mode::best_effort_large)
        assert(geom.cell_count > capacity && "reliable ring depth not strictly above capacity");
    return geom;
}

// Source-compatible overload for callers that have not yet threaded a declared mode
// and capacity: the safe default reliable ring sized to the shipped capacity floor.
[[nodiscard]] inline ring_geometry
ring_geometry_for(std::optional<std::uint32_t> max_payload) noexcept
{
    return ring_geometry_for(max_payload, ring_geometry_mode::reliable_preserving, k_max_consumers);
}

// The exact /dev/shm byte cost (control+cells region + payload slab region) a ring
// declared for this payload, mode, and capacity will allocate. Pure: no I/O, no
// allocation, no side effect. It reuses the layout helpers so the byte math has a
// single source. For wire_fallback it returns the bounded reliable ring's cost (the
// capped band), not the cost of the large payload the mode reroutes over the wire.
[[nodiscard]] inline std::size_t ring_memory_for(std::uint32_t max_payload, ring_geometry_mode mode,
                                                 std::uint32_t consumer_capacity) noexcept
{
    const ring_geometry geom = ring_geometry_for(max_payload, mode, consumer_capacity);
    return control_region_bytes(geom.cell_count) +
            slab_region_bytes(geom.cell_count, geom.slot_capacity);
}

}

#endif
