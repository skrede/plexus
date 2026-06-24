#ifndef HPP_GUARD_PLEXUS_SHM_RING_GEOMETRY_H
#define HPP_GUARD_PLEXUS_SHM_RING_GEOMETRY_H

#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/ring_layout.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::shm {

// The per-ring slab ceiling: the largest slab a single ring may size, the shipped
// default for the node-level slab-ceiling knob. The registration path (not this
// constant) fails closed above the live ceiling.
constexpr std::uint64_t k_max_ring_slab_bytes = 512ull * 1024 * 1024;

// cell_count is the power-of-two Vyukov cell depth; slot_capacity is the per-slot byte
// stride (a multiple of eight so slot i at i*stride is 8-aligned).
struct ring_geometry
{
    std::uint64_t cell_count;
    std::uint64_t slot_capacity;
};

// The reliable depth floor: a reliable ring needs depth > consumers so the slowest
// consumer always leaves the producer a free cell (the measured half-throughput stall
// at depth == consumers).
constexpr std::uint64_t next_pow2_strictly_above(std::uint64_t v) noexcept
{
    std::uint64_t p = 1;
    while(p <= v)
        p <<= 1;
    return p;
}

// The best-effort depth: depth == consumers is admissible, trading the reliable
// headroom for low memory.
constexpr std::uint64_t next_pow2_at_least(std::uint64_t v) noexcept
{
    std::uint64_t p = 1;
    while(p < v)
        p <<= 1;
    return p;
}

// Trade depth for width so per-ring memory stays bounded as the slot grows. Beyond the
// fixed-depth tiers the slot dominates, so the depth pins to the capacity floor.
inline ring_geometry ring_geometry_tier(std::uint64_t want, std::uint64_t slot, ring_geometry_mode mode, std::uint64_t capacity) noexcept
{
    if(want > 131072)
    {
        const std::uint64_t depth = mode == ring_geometry_mode::best_effort_large ? next_pow2_at_least(capacity) : next_pow2_strictly_above(capacity);
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

// Maps a topic's declared max payload, mode, and consumer capacity to a ring geometry.
// A pure layout query: it never bounds the slab itself (the fail-closed registration
// path owns that bound + diagnostic).
inline ring_geometry ring_geometry_for(std::optional<std::uint32_t> max_payload, ring_geometry_mode mode, std::uint32_t consumer_capacity) noexcept
{
    const std::uint64_t capacity = consumer_capacity == 0 ? k_max_consumers : consumer_capacity;
    const std::uint64_t want     = max_payload.value_or(0);
    ring_geometry geom           = ring_geometry_tier(want, round_up_8(want), mode, capacity);
    geom.slot_capacity           = round_up_8(geom.slot_capacity);
    if(mode != ring_geometry_mode::best_effort_large)
        assert(geom.cell_count > capacity && "reliable ring depth not strictly above capacity");
    return geom;
}

inline ring_geometry ring_geometry_for(std::optional<std::uint32_t> max_payload) noexcept
{
    return ring_geometry_for(max_payload, ring_geometry_mode::reliable_preserving, k_max_consumers);
}

// The exact byte cost (control+cells region + payload slab region) a ring declared for
// this payload, mode, and capacity allocates. For wire_fallback it returns the bounded
// reliable ring's cost (the capped band), not the rerouted large payload.
inline std::size_t ring_memory_for(std::uint32_t max_payload, ring_geometry_mode mode, std::uint32_t consumer_capacity) noexcept
{
    const ring_geometry geom = ring_geometry_for(max_payload, mode, consumer_capacity);
    return control_region_bytes(geom.cell_count) + slab_region_bytes(geom.cell_count, geom.slot_capacity);
}

}

#endif
