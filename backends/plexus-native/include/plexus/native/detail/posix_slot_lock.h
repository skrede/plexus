#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_POSIX_SLOT_LOCK_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_POSIX_SLOT_LOCK_H

#include "plexus/shm/slot_liveness.h"

#include <string>
#include <cstdint>

namespace plexus::native::detail {

// Slot locks live on the region's own descriptor, or, where the host's shared-memory descriptors take no record lock,
// on the ring's sidecar lock file, which the first call that needs it opens into `sidecar_fd`. `canonical` is the
// region's broker name.
plexus::shm::slot_hold hold_slot_lock(int region_fd, int &sidecar_fd, const std::string &canonical, std::uint32_t slot) noexcept;

void release_slot_lock(int region_fd, int sidecar_fd, std::uint32_t slot) noexcept;

// True as well when the host offers no proof, so a caller never declares a holder dead without one.
bool slot_locked_elsewhere(int region_fd, int &sidecar_fd, const std::string &canonical, std::uint32_t slot) noexcept;

// Every slot in one lock over their byte range, so it is held only when no other description holds any of them.
plexus::shm::slot_hold hold_all_slot_locks(int region_fd, int &sidecar_fd, const std::string &canonical) noexcept;

void release_all_slot_locks(int region_fd, int sidecar_fd) noexcept;

// Removes the region's sidecar lock file where the host uses one.
void unlink_sidecar(const std::string &canonical) noexcept;

}

#endif
