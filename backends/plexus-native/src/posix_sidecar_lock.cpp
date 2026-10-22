#include "plexus/native/detail/posix_slot_lock.h"

#include <array>
#include <cstdio>
#include <cstddef>

#include <cerrno>

#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>

namespace plexus::native::detail {

namespace {

using plexus::shm::slot_hold;

constexpr std::uint32_t k_sidecar_attempts = 4;

// The sidecar lives in the per-user cache directory, which only a safe boot clears: $TMPDIR can be redirected, and the
// confstr man page lets the system remove a temporary-directory file not accessed for three days, which a lock file never
// is, even while held. A crash-left sidecar of a name no one acquires again stays until that safe boot, and a reboot
// ends every region it could guard.
bool sidecar_path(const std::string &canonical, std::array<char, 1024> &path) noexcept
{
#if defined(__APPLE__)
    const std::size_t counted = ::confstr(_CS_DARWIN_USER_CACHE_DIR, path.data(), path.size());
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

// O_NOFOLLOW refuses a planted symlink, and an exclusive create never follows one.
bool open_sidecar(const std::string &canonical, sidecar_lock_file &sidecar) noexcept
{
    std::array<char, 1024> path{};
    if(!sidecar_path(canonical, path))
        return false;
    constexpr int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW;
    for(std::uint32_t attempt = 0; attempt < k_sidecar_attempts; ++attempt)
    {
        sidecar.fd      = ::open(path.data(), flags | O_CREAT | O_EXCL, 0600);
        sidecar.created = sidecar.fd >= 0;
        if(sidecar.created || errno != EEXIST)
            return sidecar.created;
        sidecar.fd = ::open(path.data(), flags);
        if(sidecar.fd >= 0 || errno != ENOENT)
            return sidecar.fd >= 0;
    }
    return false;
}

// The classic lock-file rule: a lock on the descriptor guards the path only while the path still names its inode.
bool sidecar_names_inode(int fd, const std::string &canonical) noexcept
{
    std::array<char, 1024> path{};
    struct stat locked{};
    struct stat named{};
    if(!sidecar_path(canonical, path) || ::fstat(fd, &locked) != 0 || ::lstat(path.data(), &named) != 0)
        return false;
    return locked.st_dev == named.st_dev && locked.st_ino == named.st_ino;
}

void drop_sidecar(sidecar_lock_file &sidecar) noexcept
{
    ::close(sidecar.fd);
    sidecar = sidecar_lock_file{};
}

}

slot_hold hold_sidecar_range(sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t first, std::uint32_t count) noexcept
{
    for(std::uint32_t attempt = 0; attempt < k_sidecar_attempts; ++attempt)
    {
        if(sidecar.fd < 0 && !open_sidecar(canonical, sidecar))
            return slot_hold::unsupported;
        const slot_hold hold = set_record_lock(sidecar.fd, first, count);
        if(hold != slot_hold::held || sidecar_names_inode(sidecar.fd, canonical))
            return hold;
        clear_record_lock(sidecar.fd, first, count);
        drop_sidecar(sidecar);
    }
    return slot_hold::failed;
}

// A free slot is trusted only on the inode the path names: a removed sidecar holds no one's lock.
bool sidecar_slot_locked_elsewhere(sidecar_lock_file &sidecar, const std::string &canonical, std::uint32_t slot) noexcept
{
    for(std::uint32_t attempt = 0; attempt < k_sidecar_attempts; ++attempt)
    {
        if(sidecar.fd < 0 && !open_sidecar(canonical, sidecar))
            return true;
        const record_probe probe = probe_record_lock(sidecar.fd, slot);
        if(probe != record_probe::free || sidecar_names_inode(sidecar.fd, canonical))
            return probe != record_probe::free;
        drop_sidecar(sidecar);
    }
    return true;
}

void close_sidecar(sidecar_lock_file &sidecar, const std::string &canonical) noexcept
{
    if(sidecar.fd < 0)
        return;
    if(sidecar.created && set_record_lock(sidecar.fd, 0, plexus::shm::k_liveness_slots) == slot_hold::held && sidecar_names_inode(sidecar.fd, canonical))
        unlink_sidecar(canonical);
    drop_sidecar(sidecar);
}

void unlink_sidecar(const std::string &canonical) noexcept
{
    std::array<char, 1024> path{};
    if(sidecar_path(canonical, path))
        ::unlink(path.data());
}

}
