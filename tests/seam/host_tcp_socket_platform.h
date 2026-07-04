#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_PLATFORM_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_PLATFORM_H

#include <sys/socket.h>

namespace plexus::test {

// macOS/BSD have no per-send MSG_NOSIGNAL; there SIGPIPE is suppressed per-socket via SO_NOSIGPIPE
// (see suppress_sigpipe). On Linux MSG_NOSIGNAL is defined, so this flag is that value and the send
// path is byte-for-byte unchanged.
#if defined(MSG_NOSIGNAL)
inline constexpr int nosignal_send_flag = MSG_NOSIGNAL;
#else
inline constexpr int nosignal_send_flag = 0;
#endif

// Apple has no send-time MSG_NOSIGNAL; a raw send to a closed peer would raise SIGPIPE and kill the
// process. asio scopes this per-fd on its own sockets; this raw test seam sets it explicitly. On
// Linux this is a no-op (MSG_NOSIGNAL carries the suppression per send).
inline void suppress_sigpipe(int fd)
{
#if defined(__APPLE__)
    const int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#else
    (void)fd;
#endif
}

}

#endif
