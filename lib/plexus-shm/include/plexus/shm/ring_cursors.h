#ifndef HPP_GUARD_PLEXUS_SHM_RING_CURSORS_H
#define HPP_GUARD_PLEXUS_SHM_RING_CURSORS_H

#include "plexus/shm/ring_header.h"

#include <atomic>
#include <cstdint>
#include <optional>

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

// A caller may pass a capacity read from the peer-written header, so an index is also held inside the cursor array.
inline bool cursor_in_range(std::uint64_t consumer_capacity, std::uint32_t index) noexcept
{
    return index < consumer_capacity && index < k_max_consumers;
}

// The registered-consumer high-water as a scan bound: the header is peer-written, so a scan never trusts it past the
// cursor array.
inline std::uint32_t cursor_scan_bound(const control_header_t &h) noexcept
{
    const std::uint32_t hw = h.high_water.load(std::memory_order_acquire);
    return hw < k_max_consumers ? hw : static_cast<std::uint32_t>(k_max_consumers);
}

// The active word of a cursor a producer is reclaiming: it still gates, and neither a claim nor its owner's unregister
// can move it, so the reclaimer's liveness probe and its clearing stores cannot land on a newcomer's cursor.
inline constexpr std::uint32_t k_cursor_reclaiming = 2;

// The active word of a cursor whose claim is advancing its generation: while active reads it, no release or reclaim acts
// on the cursor, so a release's idle store always follows the claim's waiting word.
inline constexpr std::uint32_t k_cursor_claiming = 3;

// Claims cursor `index` and returns the claim's generation; nullopt when another consumer has it. The cursor starts at
// the claim's waiting word and gates nothing until it is placed or published.
inline std::optional<std::uint32_t> claim_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index) noexcept
{
    std::uint32_t expected = 0;
    if(!cursor_in_range(consumer_capacity, index) || !h.cursors[index].active.compare_exchange_strong(expected, k_cursor_claiming, std::memory_order_acq_rel))
        return std::nullopt;
    const std::uint32_t generation = h.cursors[index].generation.fetch_add(1, std::memory_order_relaxed) + 1;
    // The waiting word carries the generation just advanced, and it lands before active 1, so a release's idle store
    // can only follow it.
    h.cursors[index].position.store(cursor_waiting(generation), std::memory_order_release);
    h.cursors[index].active.store(1, std::memory_order_release);
    raise_high_water(h, index + 1);
    return generation;
}

// The first free cursor's claim generation, its index in out_index; nullopt when consumer_capacity slots are registered.
inline std::optional<std::uint32_t> register_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t &out_index) noexcept
{
    for(std::uint32_t i = 0; i < consumer_capacity; ++i)
    {
        if(const std::optional<std::uint32_t> generation = claim_cursor(h, consumer_capacity, i))
        {
            out_index = i;
            return generation;
        }
    }
    return std::nullopt;
}

// A cursor a reclaimer has marked is left to it: the idle position no longer gates, and a reclaim pass frees the cursor
// once the owner's slot is free.
inline void unregister_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index) noexcept
{
    if(!cursor_in_range(consumer_capacity, index))
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
    if(!cursor_in_range(consumer_capacity, index) || !h.cursors[index].active.compare_exchange_strong(expected, k_cursor_reclaiming, std::memory_order_acq_rel))
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

// Places claim generation's waiting reader at position; false when the slot does not hold that waiting word. A release
// or re-claim replaces the word, so a stale placement's swap fails.
inline bool place_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index, std::uint32_t generation, std::uint64_t position) noexcept
{
    if(!cursor_in_range(consumer_capacity, index))
        return false;
    std::uint64_t waiting = cursor_waiting(generation);
    return h.cursors[index].position.compare_exchange_strong(waiting, position, std::memory_order_acq_rel);
}

// Released so the producer's reclamation gate observes it.
inline void publish_cursor(control_header_t &h, std::uint64_t consumer_capacity, std::uint32_t index, std::uint64_t position) noexcept
{
    if(!cursor_in_range(consumer_capacity, index))
        return;
    h.cursors[index].position.store(position, std::memory_order_release);
}

// The slowest registered consumer cursor that holds a read position, or k_cursor_idle
// when none is gating. It is the producer back-pressure progress signal: this
// value advances exactly when the lagging consumer that holds the reliable gate
// closed drains a slot, so a blocking producer can tell a live-but-slow consumer
// (the position moved) from a wedged/dead one (it did not).
inline std::uint64_t slowest_consumer_position(const control_header_t &h) noexcept
{
    std::uint64_t slowest    = k_cursor_idle;
    const std::uint32_t scan = cursor_scan_bound(h);
    for(std::uint32_t i = 0; i < scan; ++i)
    {
        if(h.cursors[i].active.load(std::memory_order_acquire) == 0)
            continue;
        const std::uint64_t cur = h.cursors[i].position.load(std::memory_order_acquire);
        if(cursor_holds_position(cur) && cur < slowest)
            slowest = cur;
    }
    return slowest;
}

// True when every registered cursor that holds a read position has consumed the occupant the
// claim at `position` would overwrite (the one at position - cell_count).
inline bool consumers_passed(const control_header_t &h, std::uint64_t cell_count, std::uint64_t position) noexcept
{
    if(position < cell_count)
        return true; // first lap: no prior occupant to free
    const std::uint64_t occupant = position - cell_count;
    const std::uint32_t scan     = cursor_scan_bound(h);
    for(std::uint32_t i = 0; i < scan; ++i)
    {
        if(h.cursors[i].active.load(std::memory_order_acquire) == 0)
            continue;
        const std::uint64_t cur = h.cursors[i].position.load(std::memory_order_acquire);
        if(cursor_holds_position(cur) && cur <= occupant)
            return false; // a registered consumer has not consumed the occupant yet
    }
    return true;
}

}

#endif
