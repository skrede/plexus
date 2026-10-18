#ifndef HPP_GUARD_PLEXUS_SHM_RING_LAYOUT_H
#define HPP_GUARD_PLEXUS_SHM_RING_LAYOUT_H

#include "plexus/shm/ring_cells.h"
#include "plexus/shm/ring_header.h"

#include <cstddef>

namespace plexus::shm {

// Round a byte count up to the next multiple of eight so a slot base laid out
// at i*slot_capacity, over a page-aligned slab base, is always 8-aligned.
constexpr std::size_t round_up_8(std::size_t bytes) noexcept
{
    return (bytes + 7u) & ~static_cast<std::size_t>(7u);
}

// Bytes the control+cells region must hold: the header followed by cell_count
// cache-line-aligned cells.
constexpr std::size_t control_region_bytes(std::size_t cell_count) noexcept
{
    return sizeof(control_header_t) + cell_count * sizeof(cell_t);
}

// Bytes the payload slab region must hold: cell_count fixed-stride slots, each
// slot_capacity rounded up to a multiple of eight so slot i at i*stride is
// 8-aligned.
constexpr std::size_t slab_region_bytes(std::size_t cell_count, std::size_t slot_capacity) noexcept
{
    return cell_count * round_up_8(slot_capacity);
}

}

#endif
