#ifndef HPP_GUARD_PLEXUS_SHM_SLOT_LIVENESS_H
#define HPP_GUARD_PLEXUS_SHM_SLOT_LIVENESS_H

#include "plexus/shm/ring_header.h"

#include <cstdint>
#include <concepts>

namespace plexus::shm {

// busy: another holder has the slot; unsupported: the host offers this region no kernel proof of a holder's life.
enum class slot_hold : std::uint8_t
{
    held,
    busy,
    unsupported,
    failed,
};

inline constexpr std::uint32_t k_creator_slot   = 0;
inline constexpr std::uint32_t k_liveness_slots = 1 + static_cast<std::uint32_t>(k_max_consumers);

constexpr std::uint32_t cursor_slot(std::uint32_t cursor_index) noexcept
{
    return 1 + cursor_index;
}

// A kernel proof per slot of a region that its holder is alive: the kernel drops the hold when the holder's process
// dies. A holder takes the slot before it publishes the state the slot guards and releases it only after unpublishing
// that state, so published state whose slot no other holder has belongs to a dead process. hold_all_slots takes every
// slot in one step or none: held means no other holder has any slot of the region.
template<typename H>
concept slot_liveness = requires(H &handle, const H &probe, std::uint32_t slot) {
    { handle.hold_slot(slot) } -> std::same_as<slot_hold>;
    handle.release_slot(slot);
    { probe.slot_held_elsewhere(slot) } -> std::same_as<bool>;
    { handle.hold_all_slots() } -> std::same_as<slot_hold>;
    handle.release_all_slots();
};

}

#endif
