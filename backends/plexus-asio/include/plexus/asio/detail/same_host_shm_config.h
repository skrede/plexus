#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SAME_HOST_SHM_CONFIG_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SAME_HOST_SHM_CONFIG_H

// PLEXUS_SAME_HOST_NO_SHM forces the portable AF_UNIX + TCP branch on any host (it lets the
// non-shm composition be exercised off a Linux build host).
#if defined(__linux__) && !defined(PLEXUS_SAME_HOST_NO_SHM)
    #define PLEXUS_SAME_HOST_SHM 1
#else
    #define PLEXUS_SAME_HOST_SHM 0
#endif

#if PLEXUS_SAME_HOST_SHM
    #include "plexus/asio/shm/linux/shm_member.h"
#endif

#endif
