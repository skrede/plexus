#ifndef HPP_GUARD_PLEXUS_SHM_RING_LAYOUT_H
#define HPP_GUARD_PLEXUS_SHM_RING_LAYOUT_H

#include "plexus/shm/ring_cells.h"
#include "plexus/shm/ring_header.h"

#include <new>
#include <span>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace plexus::shm {

struct ring_claim_result
{
    std::uint64_t position{0};
    std::span<std::byte> slab;
};

struct ring_consume_result
{
    std::uint64_t position{0};
    std::span<const std::byte> slab;
};

// Hard maxima on a ring's geometry, checked before any size is computed from a peer-written header. A slot never exceeds
// the 1 GiB region bound every broker enforces, and the two maxima keep every product below in 64-bit range.
inline constexpr std::uint64_t k_max_cells         = 1ull << 16;
inline constexpr std::uint64_t k_max_slot_capacity = 1ull << 30;

static_assert((k_max_cells * (k_max_slot_capacity + 7u)) / k_max_cells == k_max_slot_capacity + 7u, "the geometry maxima must keep cells times stride in range");

// The slab's first cache line carries the ring's identity, written before the stamp, and the slots follow it.
struct alignas(k_cache_line) slab_header_t
{
    std::atomic<std::uint64_t> identity;
};

static_assert(std::is_standard_layout_v<slab_header_t>, "slab_header_t must be standard-layout for SHM placement");
static_assert(sizeof(slab_header_t) == k_cache_line, "the slab header must stay one cache line");

// The size functions below are only ever called with geometry bounded by those maxima, so none of them wraps.
// Round a byte count up to the next multiple of eight so a slot base laid out
// at i*slot_capacity, over the cache-line-aligned slot area, is always 8-aligned.
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

// Bytes of the slot area: cell_count fixed-stride slots, each slot_capacity rounded up to a multiple of eight so slot i
// at i*stride is 8-aligned.
constexpr std::size_t slab_payload_bytes(std::size_t cell_count, std::size_t slot_capacity) noexcept
{
    return cell_count * round_up_8(slot_capacity);
}

constexpr std::size_t slab_region_bytes(std::size_t cell_count, std::size_t slot_capacity) noexcept
{
    return sizeof(slab_header_t) + slab_payload_bytes(cell_count, slot_capacity);
}

// Zero for a mapping too small to hold the slab header.
inline std::uint64_t slab_identity_of(std::span<std::byte> slab) noexcept
{
    if(slab.size() < sizeof(slab_header_t))
        return 0;
    return std::launder(reinterpret_cast<slab_header_t *>(slab.data()))->identity.load(std::memory_order_relaxed);
}

}

#endif
