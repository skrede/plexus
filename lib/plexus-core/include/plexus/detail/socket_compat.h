#ifndef HPP_GUARD_PLEXUS_DETAIL_SOCKET_COMPAT_H
#define HPP_GUARD_PLEXUS_DETAIL_SOCKET_COMPAT_H

#include "plexus/io/security/peer_cred_policy.h"

#include <cerrno>
#include <string>
#include <cstdint>
#include <filesystem>
#include <system_error>

#if !defined(_WIN32)
    #include <unistd.h>
    #include <sys/stat.h>
    #include <sys/socket.h>
#endif

namespace plexus::detail {

// not-found is success — a stale inode left by a crashed prior run is cleared, ENOENT ignored.
inline void remove_socket_path(const std::string &path)
{
    std::error_code ec;
    (void)std::filesystem::remove(path, ec);
}

#if !defined(_WIN32)
using socket_mode                                = ::mode_t;
inline constexpr socket_mode default_socket_mode = S_IRWXU;
#else
using socket_mode                                = std::uint32_t;
inline constexpr socket_mode default_socket_mode = 0;
#endif

#if !defined(_WIN32)

// Bind under a restrictive umask so the socket inode is created with no group/other access from the
// outset, closing the TOCTOU window between bind and the mode application; the prior mask restores
// on scope exit.
class scoped_bind_umask
{
public:
    scoped_bind_umask()
            : m_prev(::umask(0077))
    {
    }

    scoped_bind_umask(const scoped_bind_umask &)            = delete;
    scoped_bind_umask &operator=(const scoped_bind_umask &) = delete;

    ~scoped_bind_umask()
    {
        (void)::umask(m_prev);
    }

private:
    ::mode_t m_prev;
};

inline bool apply_socket_mode(const std::string &path, socket_mode mode, std::error_code &ec)
{
    if(::chmod(path.c_str(), mode) != 0)
    {
        ec = std::error_code(errno, std::generic_category());
        return false;
    }
    return true;
}

#else

class scoped_bind_umask
{
public:
    scoped_bind_umask()                                     = default;
    scoped_bind_umask(const scoped_bind_umask &)            = delete;
    scoped_bind_umask &operator=(const scoped_bind_umask &) = delete;
};

// Windows AF_UNIX carries no mode bits; the NTFS ACL governs access instead.
inline bool apply_socket_mode(const std::string &, socket_mode, std::error_code &)
{
    return true;
}

#endif

#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__)
inline constexpr bool peer_cred_supported = true;
#else
inline constexpr bool peer_cred_supported = false;
#endif

#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__)
// A false return means the read FAILED — the caller refuses (fail-closed); it is distinct from
// peer_cred_supported being false, which means the platform exposes no creds at all.
inline bool read_peer_cred(int native_handle, io::security::peer_cred &out)
{
    #if defined(__linux__)
    ::ucred cred{};
    ::socklen_t len = sizeof(cred);
    if(::getsockopt(native_handle, SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0)
        return false;
    out = io::security::peer_cred{static_cast<std::uint32_t>(cred.uid), static_cast<std::uint32_t>(cred.gid), static_cast<std::uint32_t>(cred.pid)};
    return true;
    #else
    ::uid_t uid = 0;
    ::gid_t gid = 0;
    if(::getpeereid(native_handle, &uid, &gid) != 0) // no pid on Darwin/BSD => pid=0
        return false;
    out = io::security::peer_cred{static_cast<std::uint32_t>(uid), static_cast<std::uint32_t>(gid), 0};
    return true;
    #endif
}
#else
// unix_accept's admit_peer names read_peer_cred inside `if constexpr(peer_cred_supported)`;
// two-phase lookup still parses that discarded branch on a platform without peer creds, so the
// name must be declared. A false return is fail-closed by the same contract as a read failure.
inline bool read_peer_cred(int, io::security::peer_cred &)
{
    return false;
}
#endif

}

#endif
