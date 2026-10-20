#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_POSIX_SLOT_LOCK_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_POSIX_SLOT_LOCK_H

#include "plexus/shm/slot_liveness.h"

#include <string>
#include <cstdint>

namespace plexus::native::detail {

// Where the host's shared-memory descriptors take no record lock, a region's slot locks live on its sidecar lock file,
// which the first slot call that needs it opens. `created` marks the handle whose exclusive create made the file.
struct sidecar_lock_file
{
    int fd       = -1;
    bool created = false;
};

enum class record_probe : std::uint8_t
{
    free,
    held,
    unsupported,
};

plexus::shm::slot_hold set_record_lock(int fd, std::uint32_t first, std::uint32_t count) noexcept;

void clear_record_lock(int fd, std::uint32_t first, std::uint32_t count) noexcept;

// Reports only a lock another open file description holds.
record_probe probe_record_lock(int fd, std::uint32_t slot) noexcept;

// `canonical` is the region's broker name.
plexus::shm::slot_hold hold_slot_lock(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t slot) noexcept;

void release_slot_lock(int region_fd, const sidecar_lock_file &sidecar, std::uint32_t slot) noexcept;

// True as well when the host offers no proof, so a caller never declares a holder dead without one.
bool slot_locked_elsewhere(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t slot) noexcept;

// Every slot in one lock over their byte range, so it is held only when no other description holds any of them.
plexus::shm::slot_hold hold_all_slot_locks(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical) noexcept;

void release_all_slot_locks(int region_fd, const sidecar_lock_file &sidecar) noexcept;

// A lock or probe on the sidecar, retried on a freshly opened file while the path no longer names the locked one.
plexus::shm::slot_hold hold_sidecar_range(sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t first, std::uint32_t count) noexcept;

bool sidecar_slot_locked_elsewhere(sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t slot) noexcept;

// The handle that created the sidecar removes the file first, while it holds every slot on it and the path still names it.
void close_sidecar(sidecar_lock_file &sidecar, const std::string &canonical) noexcept;

// Removes the region's sidecar lock file where the host uses one.
void unlink_sidecar(const std::string &canonical) noexcept;

}

#endif
