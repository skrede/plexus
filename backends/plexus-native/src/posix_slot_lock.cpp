#include "plexus/native/detail/posix_slot_lock.h"

#include <array>
#include <cstdio>
#include <cstddef>

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>

#if defined(__linux__) && !defined(F_OFD_SETLK)
    #error "the shared-memory slot locks need open-file-description locks (Linux 3.15)"
#endif

namespace plexus::native::detail {

namespace {

using plexus::shm::slot_hold;

enum class slot_probe : std::uint8_t
{
    free,
    held,
    unsupported,
};

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

slot_hold set_lock(int fd, std::uint32_t first, std::uint32_t count) noexcept
{
    struct flock lk = byte_lock(F_WRLCK, first, count);
    if(::fcntl(fd, F_OFD_SETLK, &lk) == 0)
        return slot_hold::held;
    if(errno == EAGAIN || errno == EACCES)
        return slot_hold::busy;
    return lock_unsupported(errno) ? slot_hold::unsupported : slot_hold::failed;
}

void clear_lock(int fd, std::uint32_t first, std::uint32_t count) noexcept
{
    struct flock lk = byte_lock(F_UNLCK, first, count);
    ::fcntl(fd, F_OFD_SETLK, &lk);
}

// F_OFD_GETLK reports only a lock another description holds; a failed query reads as held.
slot_probe probe_lock(int fd, std::uint32_t slot) noexcept
{
    struct flock lk = byte_lock(F_WRLCK, slot, 1);
    if(::fcntl(fd, F_OFD_GETLK, &lk) != 0)
        return lock_unsupported(errno) ? slot_probe::unsupported : slot_probe::held;
    return lk.l_type == F_UNLCK ? slot_probe::free : slot_probe::held;
}
#else
slot_hold set_lock(int, std::uint32_t, std::uint32_t) noexcept
{
    return slot_hold::unsupported;
}

void clear_lock(int, std::uint32_t, std::uint32_t) noexcept
{
}

slot_probe probe_lock(int, std::uint32_t) noexcept
{
    return slot_probe::unsupported;
}
#endif

// The sidecar lives under the per-user directory confstr names, not $TMPDIR, which the environment can redirect.
bool sidecar_path(const std::string &canonical, std::array<char, 1024> &path) noexcept
{
#if defined(__APPLE__)
    const std::size_t counted = ::confstr(_CS_DARWIN_USER_TEMP_DIR, path.data(), path.size());
    if(counted < 2 || counted > path.size())
        return false;
    std::size_t used = counted - 1;
    if(path[used - 1] != '/')
        path[used++] = '/';
    const int n = std::snprintf(path.data() + used, path.size() - used, "%s.lk", canonical.c_str() + 1);
    return n >= 0 && static_cast<std::size_t>(n) < path.size() - used;
#else
    (void)canonical;
    (void)path;
    return false;
#endif
}

// O_NOFOLLOW refuses a planted symlink.
int open_sidecar(const std::string &canonical) noexcept
{
    std::array<char, 1024> path{};
    if(!sidecar_path(canonical, path))
        return -1;
    return ::open(path.data(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
}

bool ensure_sidecar(int &sidecar_fd, const std::string &canonical) noexcept
{
    if(sidecar_fd < 0)
        sidecar_fd = open_sidecar(canonical);
    return sidecar_fd >= 0;
}

slot_hold hold_lock_range(int region_fd, int &sidecar_fd, const std::string &canonical, std::uint32_t first, std::uint32_t count) noexcept
{
    if(sidecar_fd < 0)
    {
        if(const slot_hold hold = set_lock(region_fd, first, count); hold != slot_hold::unsupported)
            return hold;
        if(!ensure_sidecar(sidecar_fd, canonical))
            return slot_hold::unsupported;
    }
    return set_lock(sidecar_fd, first, count);
}

}

slot_hold hold_slot_lock(int region_fd, int &sidecar_fd, const std::string &canonical, std::uint32_t slot) noexcept
{
    return hold_lock_range(region_fd, sidecar_fd, canonical, slot, 1);
}

void release_slot_lock(int region_fd, int sidecar_fd, std::uint32_t slot) noexcept
{
    clear_lock(sidecar_fd >= 0 ? sidecar_fd : region_fd, slot, 1);
}

bool slot_locked_elsewhere(int region_fd, int &sidecar_fd, const std::string &canonical, std::uint32_t slot) noexcept
{
    if(sidecar_fd < 0)
    {
        if(const slot_probe probe = probe_lock(region_fd, slot); probe != slot_probe::unsupported)
            return probe == slot_probe::held;
        if(!ensure_sidecar(sidecar_fd, canonical))
            return true;
    }
    return probe_lock(sidecar_fd, slot) != slot_probe::free;
}

slot_hold hold_all_slot_locks(int region_fd, int &sidecar_fd, const std::string &canonical) noexcept
{
    return hold_lock_range(region_fd, sidecar_fd, canonical, 0, plexus::shm::k_liveness_slots);
}

void release_all_slot_locks(int region_fd, int sidecar_fd) noexcept
{
    clear_lock(sidecar_fd >= 0 ? sidecar_fd : region_fd, 0, plexus::shm::k_liveness_slots);
}

void unlink_sidecar(const std::string &canonical) noexcept
{
    std::array<char, 1024> path{};
    if(sidecar_path(canonical, path))
        ::unlink(path.data());
}

}
