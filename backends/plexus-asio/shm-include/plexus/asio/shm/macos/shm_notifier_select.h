#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SHM_NOTIFIER_SELECT_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SHM_NOTIFIER_SELECT_H

// 0 = named semaphore + self-pipe doorbell; 1 = named semaphore + private-kqueue EVFILT_USER
// doorbell. The on-host benchmark keeps the winner by flipping this one define (then deleting the
// unused header and this branch).
#ifndef PLEXUS_MACOS_SHM_NOTIFIER_KQUEUE
    #define PLEXUS_MACOS_SHM_NOTIFIER_KQUEUE 0
#endif

#if PLEXUS_MACOS_SHM_NOTIFIER_KQUEUE
    #include "plexus/asio/shm/macos/kqueue_notifier.h"
#else
    #include "plexus/asio/shm/macos/sem_notifier.h"
#endif

namespace plexus::asio::shm {

#if PLEXUS_MACOS_SHM_NOTIFIER_KQUEUE
template<typename P>
using mac_notifier = kqueue_notifier<P>;
#else
template<typename P>
using mac_notifier = sem_notifier<P>;
#endif

}

#endif
