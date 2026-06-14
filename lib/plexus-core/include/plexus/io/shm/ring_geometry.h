#ifndef HPP_GUARD_PLEXUS_IO_SHM_RING_GEOMETRY_H
#define HPP_GUARD_PLEXUS_IO_SHM_RING_GEOMETRY_H

#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/ring_layout.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io::shm {

// The per-ring slab ceiling: the largest /dev/shm slab a single ring may size, so an
// oversize payload declaration cannot map an unbounded region. It is the layout-side
// bound (cap * floor), independent of the decode layer's upstream max_payload cap. It
// is the SHIPPED fallback ceiling; the live ceiling is a node_options knob threaded to
// the registry, with this constant the ultimate default.
constexpr std::uint64_t k_max_ring_slab_bytes = 16ull * 1024 * 1024;

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

// Maps a topic's declared maximum payload, geometry mode, and per-ring consumer
// capacity to a ring geometry, trading depth for width so per-ring memory stays
// bounded as the slot grows. An unset or small declaration keeps the default
// deep-but-narrow ring; a larger declaration gets a proportionally shallower ring
// wide enough to fit the payload, with the deepest band's depth derived from the
// declared capacity rather than a fixed floor. cell_count is always a power of two
// and, under the two reliable-geometry modes (reliable_preserving, wire_fallback),
// strictly exceeds the declared capacity; best_effort_large admits depth ==
// capacity. The total per-ring slab is asserted under a fixed ceiling so an oversize
// declaration cannot size an unbounded slab; the over-ceiling reliable case is left
// for the caller to detect (the fail-closed registration path owns the diagnostic).
[[nodiscard]] inline ring_geometry ring_geometry_for(std::optional<std::uint32_t> max_payload,
                                                     ring_geometry_mode mode,
                                                     std::uint32_t consumer_capacity) noexcept
{
    const std::uint64_t capacity = consumer_capacity == 0 ? k_max_consumers : consumer_capacity;
    const std::uint64_t want = max_payload.value_or(0);
    const std::uint64_t slot = round_up_8(want);

    ring_geometry geom{256, 4096};
    if(want > 131072)
    {
        // Beyond the fixed-depth tiers the slot dominates the per-ring slab. The
        // reliable-geometry modes pin the depth to the capacity floor (> capacity);
        // best_effort_large admits depth == capacity for the same low memory the old
        // depth-16 deep tier gave.
        const std::uint64_t depth = mode == ring_geometry_mode::best_effort_large
                                        ? next_pow2_at_least(capacity)
                                        : next_pow2_strictly_above(capacity);
        geom = ring_geometry{depth, slot};
    }
    else if(want > 65536)
        geom = ring_geometry{64, slot};
    else if(want > 32768)
        geom = ring_geometry{128, slot};
    else if(want > 4096)
        geom = ring_geometry{256, slot};

    geom.slot_capacity = round_up_8(geom.slot_capacity);
    if(mode != ring_geometry_mode::best_effort_large)
        assert(geom.cell_count > capacity && "reliable ring depth not strictly above capacity");
    assert(geom.cell_count * geom.slot_capacity <= k_max_ring_slab_bytes && "ring slab over ceiling");
    return geom;
}

// Source-compatible overload for callers that have not yet threaded a declared mode
// and capacity: the safe default reliable ring sized to the shipped capacity floor.
[[nodiscard]] inline ring_geometry ring_geometry_for(std::optional<std::uint32_t> max_payload) noexcept
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
