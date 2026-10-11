#ifndef HPP_GUARD_PLEXUS_SHM_WAITER_PARK_H
#define HPP_GUARD_PLEXUS_SHM_WAITER_PARK_H

#include "plexus/shm/ring_layout.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::shm {

// A per-waiter thread parker (the std/parking_lot EMPTY/PARKED/NOTIFIED word) per cursor slot: a waiter blocks on its
// own word while it reads PARKED. The waiter's park store + generation load and the producer's generation bump +
// park read are a Dekker store-buffering pair, so all four are seq_cst: either the waiter's recheck sees the bump or
// the producer sees PARKED.

inline std::atomic<std::uint32_t> &park_word(control_header_t &header, std::uint32_t slot) noexcept
{
    return header.park[slot];
}

inline std::uint32_t bump_generation(control_header_t &header) noexcept
{
    return header.notify_generation.fetch_add(1, std::memory_order_seq_cst) + 1;
}

// The plain load first keeps the park line shared while nobody is parked: only a PARKED word pays for the exchange.
inline bool notify_if_parked(control_header_t &header, std::uint32_t slot) noexcept
{
    std::atomic<std::uint32_t> &park = park_word(header, slot);
    std::uint32_t expected           = k_park_parked;
    return park.load(std::memory_order_seq_cst) == k_park_parked && park.compare_exchange_strong(expected, k_park_notified, std::memory_order_seq_cst);
}

// Bounded by the attach-validated capacity and by k_max_consumers, so a header a peer rewrote after attach can never
// index past the cursor array. A slot's cursor line is read only once its word reads PARKED, so the scan of busy
// waiters touches the park line alone.
inline std::uint32_t collect_parked(control_header_t &header) noexcept
{
    const std::size_t high_water = header.high_water.load(std::memory_order_acquire);
    const std::size_t bound      = std::min({high_water, header.cursor_region_capacity(), k_max_consumers});
    std::uint32_t mask           = 0;
    for(std::uint32_t slot = 0; slot < bound; ++slot)
    {
        if(park_word(header, slot).load(std::memory_order_seq_cst) != k_park_parked || header.cursors[slot].active.load(std::memory_order_acquire) == 0)
            continue;
        if(notify_if_parked(header, slot))
            mask |= std::uint32_t{1} << slot;
    }
    return mask;
}

inline std::uint32_t signal_parked(control_header_t &header) noexcept
{
    bump_generation(header);
    return collect_parked(header);
}

template<typename Wake>
void wake_parked(control_header_t &header, std::uint32_t mask, Wake &&wake)
{
    for(std::uint32_t slot = 0; mask != 0; ++slot, mask >>= 1)
    {
        if((mask & 1u) != 0)
            wake(park_word(header, slot));
    }
}

inline std::uint32_t park_snapshot(const control_header_t &header) noexcept
{
    return header.notify_generation.load(std::memory_order_acquire);
}

inline void park_store(control_header_t &header, std::uint32_t slot) noexcept
{
    park_word(header, slot).store(k_park_parked, std::memory_order_seq_cst);
}

inline bool park_recheck(const control_header_t &header, std::uint32_t snapshot) noexcept
{
    return header.notify_generation.load(std::memory_order_seq_cst) == snapshot;
}

inline void park_clear(control_header_t &header, std::uint32_t slot) noexcept
{
    park_word(header, slot).store(k_park_empty, std::memory_order_release);
}

}

#endif
