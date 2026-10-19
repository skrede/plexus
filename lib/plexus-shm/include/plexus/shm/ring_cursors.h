#ifndef HPP_GUARD_PLEXUS_SHM_RING_CURSORS_H
#define HPP_GUARD_PLEXUS_SHM_RING_CURSORS_H

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"

#include <atomic>
#include <cstdint>

namespace plexus::shm::detail {

// Monotonically raise the registered-consumer high-water to at least `want`.
// A fresh cursor joins at tail_position(), so it cannot gate a reclaimable
// occupant until a full ring of enqueues has elapsed -- ample release/acquire
// edges for this release bump to reach the producer's acquire-bounded scan
// before the cursor can hold back reclamation.
inline void raise_high_water(control_header_t &h, std::uint32_t want) noexcept
{
    std::uint32_t hw = h.high_water.load(std::memory_order_relaxed);
    while(hw < want && !h.high_water.compare_exchange_weak(hw, want, std::memory_order_release, std::memory_order_relaxed))
        ;
}

// The active word of a cursor a producer is reclaiming: it still gates, and neither a claim nor its owner's unregister
// can move it, so the reclaimer's liveness probe and its clearing stores cannot land on a newcomer's cursor.
inline constexpr std::uint32_t k_cursor_reclaiming = 2;

// Claims cursor `index` (it starts at k_cursor_idle, not gating); false when another consumer has it.
inline bool claim_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index) noexcept
{
    std::uint32_t expected = 0;
    if(index >= consumer_capacity || !h.cursors[index].active.compare_exchange_strong(expected, 1, std::memory_order_acq_rel))
        return false;
    h.cursors[index].position.store(k_cursor_idle, std::memory_order_release);
    raise_high_water(h, index + 1);
    return true;
}

// Rejected when consumer_capacity slots are already registered.
inline loan_status register_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t &out_index) noexcept
{
    for(std::uint32_t i = 0; i < consumer_capacity; ++i)
    {
        if(claim_cursor(h, consumer_capacity, i))
        {
            out_index = i;
            return loan_status::ok;
        }
    }
    return loan_status::rejected;
}

// A cursor a reclaimer has marked is left to it: the idle position no longer gates, and a reclaim pass frees the cursor
// once the owner's slot is free.
inline void unregister_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index) noexcept
{
    if(index >= consumer_capacity)
        return;
    h.cursors[index].position.store(k_cursor_idle, std::memory_order_release);
    std::uint32_t expected = 1;
    h.cursors[index].active.compare_exchange_strong(expected, 0, std::memory_order_acq_rel);
}

// Frees cursor `index` when owner_alive(index) proves its owner dead; an inactive or already reclaimed index is left.
template<typename Alive>
bool reclaim_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index, Alive &owner_alive) noexcept
{
    std::uint32_t expected = 1;
    if(index >= consumer_capacity || !h.cursors[index].active.compare_exchange_strong(expected, k_cursor_reclaiming, std::memory_order_acq_rel))
        return false;
    if(owner_alive(index))
    {
        h.cursors[index].active.store(1, std::memory_order_release);
        return false;
    }
    h.cursors[index].position.store(k_cursor_idle, std::memory_order_release);
    h.cursors[index].active.store(0, std::memory_order_release);
    return true;
}

// Released so the producer's reclamation gate observes it.
inline void publish_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index, std::uint64_t position) noexcept
{
    if(index >= consumer_capacity)
        return;
    h.cursors[index].position.store(position, std::memory_order_release);
}

// The slowest registered (non-idle) consumer cursor position, or k_cursor_idle
// when none is gating. It is the producer back-pressure progress signal: this
// value advances exactly when the lagging consumer that holds the reliable gate
// closed drains a slot, so a blocking producer can tell a live-but-slow consumer
// (the position moved) from a wedged/dead one (it did not).
inline std::uint64_t slowest_consumer_position(const control_header_t &h) noexcept
{
    std::uint64_t slowest    = k_cursor_idle;
    const std::uint32_t scan = h.high_water.load(std::memory_order_acquire);
    for(std::uint32_t i = 0; i < scan; ++i)
    {
        if(h.cursors[i].active.load(std::memory_order_acquire) == 0)
            continue;
        const std::uint64_t cur = h.cursors[i].position.load(std::memory_order_acquire);
        if(cur != k_cursor_idle && cur < slowest)
            slowest = cur;
    }
    return slowest;
}

// True when every registered, non-idle cursor has consumed the occupant the
// claim at `position` would overwrite (the one at position - cell_count).
inline bool consumers_passed(const control_header_t &h, std::uint64_t cell_count, std::uint64_t position) noexcept
{
    if(position < cell_count)
        return true; // first lap: no prior occupant to free
    const std::uint64_t occupant = position - cell_count;
    const std::uint32_t scan     = h.high_water.load(std::memory_order_acquire);
    for(std::uint32_t i = 0; i < scan; ++i)
    {
        if(h.cursors[i].active.load(std::memory_order_acquire) == 0)
            continue;
        const std::uint64_t cur = h.cursors[i].position.load(std::memory_order_acquire);
        if(cur != k_cursor_idle && cur <= occupant)
            return false; // a registered consumer has not consumed the occupant yet
    }
    return true;
}

}

#endif
