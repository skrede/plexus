#ifndef HPP_GUARD_PLEXUS_IO_SHM_RING_GEOMETRY_H
#define HPP_GUARD_PLEXUS_IO_SHM_RING_GEOMETRY_H

#include "plexus/io/shm/ring_layout.h"

#include <cassert>
#include <cstdint>
#include <optional>

namespace plexus::io::shm {

// The depth + per-slot stride a broadcast ring is created with. cell_count is the
// power-of-two Vyukov cell depth; slot_capacity is the per-slot byte stride
// (always a multiple of eight so slot i at i*stride is 8-aligned).
struct ring_geometry
{
    std::uint64_t cell_count;
    std::uint64_t slot_capacity;
};

// Maps a topic's declared maximum payload to a ring geometry, trading depth for
// width so per-ring memory stays bounded as the slot grows. An unset or small
// declaration keeps the default deep-but-narrow ring; a larger declaration gets a
// proportionally shallower ring wide enough to fit the payload. cell_count is
// always a power of two and never drops below the broadcast consumer bound
// (k_max_consumers), and the total per-ring slab is asserted under a fixed ceiling
// so an oversize declaration cannot size an unbounded slab.
[[nodiscard]] inline ring_geometry ring_geometry_for(std::optional<std::uint32_t> max_payload) noexcept
{
    // Per-ring slab ceiling: an oversize declaration must not size an unbounded
    // /dev/shm region. The decode layer caps max_payload upstream; this is the
    // independent layout-side bound (cap * floor).
    constexpr std::uint64_t k_max_ring_slab_bytes = 16ull * 1024 * 1024;

    const std::uint64_t want = max_payload.value_or(0);
    const std::uint64_t slot = round_up_8(want);

    ring_geometry geom{256, 4096};
    if(want > 131072)
    {
        // Beyond the fixed-depth tiers the slot dominates the per-ring slab, so
        // the depth is the largest power of two that keeps cell_count * slot under
        // the ceiling: 32 holds up to a 512 KiB slot (8 MiB), stepping to 16 above
        // that through the 1 MiB decode cap (16 MiB at the cap).
        const std::uint64_t depth = 32ull * slot <= k_max_ring_slab_bytes ? 32u : 16u;
        geom = ring_geometry{depth, slot};
    }
    else if(want > 65536)
        geom = ring_geometry{64, slot};
    else if(want > 32768)
        geom = ring_geometry{128, slot};
    else if(want > 4096)
        geom = ring_geometry{256, slot};

    geom.slot_capacity = round_up_8(geom.slot_capacity);
    assert(geom.cell_count >= k_max_consumers && "ring depth below broadcast bound");
    assert(geom.cell_count * geom.slot_capacity <= k_max_ring_slab_bytes && "ring slab over ceiling");
    return geom;
}

}

#endif
