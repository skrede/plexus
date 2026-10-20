#include "plexus/native/detail/posix_slot_lock.h"

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>

#if defined(__linux__) && !defined(F_OFD_SETLK)
    #error "the shared-memory slot locks need open-file-description locks (Linux 3.15)"
#endif

namespace plexus::native::detail {

namespace {

using plexus::shm::slot_hold;

// A record lock refused for the descriptor's kind rather than for a conflict: macOS rejects one on a POSIX
// shared-memory descriptor, which is not a vnode.
bool lock_unsupported(int e) noexcept
{
    if(e == EINVAL || e == EBADF || e == ENOTSUP)
        return true;
    return e == EOPNOTSUPP;
}

#if defined(F_OFD_SETLK)
// An open-file-description lock belongs to the description, not the process: it ends with the description's last close,
// hence with its holder's death, and involves no pid, so pid reuse cannot fool it (man7.org F_OFD_SETLK(2const)).
struct flock byte_lock(int type, std::uint32_t first, std::uint32_t count) noexcept
{
    struct flock lk{};
    lk.l_type   = static_cast<decltype(lk.l_type)>(type);
    lk.l_whence = SEEK_SET;
    lk.l_start  = static_cast<off_t>(first);
    lk.l_len    = static_cast<off_t>(count);
    return lk;
}
#endif

slot_hold hold_lock_range(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t first, std::uint32_t count) noexcept
{
    if(sidecar.fd < 0)
    {
        if(const slot_hold hold = set_record_lock(region_fd, first, count); hold != slot_hold::unsupported)
            return hold;
    }
    return hold_sidecar_range(sidecar, canonical, first, count);
}

}

#if defined(F_OFD_SETLK)
slot_hold set_record_lock(int fd, std::uint32_t first, std::uint32_t count) noexcept
{
    struct flock lk = byte_lock(F_WRLCK, first, count);
    if(::fcntl(fd, F_OFD_SETLK, &lk) == 0)
        return slot_hold::held;
    if(errno == EAGAIN || errno == EACCES)
        return slot_hold::busy;
    return lock_unsupported(errno) ? slot_hold::unsupported : slot_hold::failed;
}

void clear_record_lock(int fd, std::uint32_t first, std::uint32_t count) noexcept
{
    struct flock lk = byte_lock(F_UNLCK, first, count);
    ::fcntl(fd, F_OFD_SETLK, &lk);
}

// A failed query reads as held.
record_probe probe_record_lock(int fd, std::uint32_t slot) noexcept
{
    struct flock lk = byte_lock(F_WRLCK, slot, 1);
    if(::fcntl(fd, F_OFD_GETLK, &lk) != 0)
        return lock_unsupported(errno) ? record_probe::unsupported : record_probe::held;
    return lk.l_type == F_UNLCK ? record_probe::free : record_probe::held;
}
#else
slot_hold set_record_lock(int, std::uint32_t, std::uint32_t) noexcept
{
    return slot_hold::unsupported;
}

void clear_record_lock(int, std::uint32_t, std::uint32_t) noexcept
{
}

record_probe probe_record_lock(int, std::uint32_t) noexcept
{
    return record_probe::unsupported;
}
#endif

slot_hold hold_slot_lock(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t slot) noexcept
{
    return hold_lock_range(region_fd, sidecar, canonical, slot, 1);
}

void release_slot_lock(int region_fd, const sidecar_lock_file &sidecar, std::uint32_t slot) noexcept
{
    clear_record_lock(sidecar.fd >= 0 ? sidecar.fd : region_fd, slot, 1);
}

bool slot_locked_elsewhere(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t slot) noexcept
{
    if(sidecar.fd < 0)
    {
        if(const record_probe probe = probe_record_lock(region_fd, slot); probe != record_probe::unsupported)
            return probe == record_probe::held;
    }
    return sidecar_slot_locked_elsewhere(sidecar, canonical, slot);
}

slot_hold hold_all_slot_locks(int region_fd, sidecar_lock_file &sidecar, const std::string &canonical) noexcept
{
    return hold_lock_range(region_fd, sidecar, canonical, 0, plexus::shm::k_liveness_slots);
}

void release_all_slot_locks(int region_fd, const sidecar_lock_file &sidecar) noexcept
{
    clear_record_lock(sidecar.fd >= 0 ? sidecar.fd : region_fd, 0, plexus::shm::k_liveness_slots);
}

}
