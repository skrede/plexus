#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_SHM_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_SHM_NOTIFIER_H

#if defined(_WIN32)
    #include "plexus/asio/shm/windows/event_notifier.h"
#elif defined(__APPLE__)
    #include "plexus/asio/shm/macos/sem_notifier.h"
#else
    #include "plexus/asio/shm/linux/ring_notifier.h"
#endif

namespace plexus::asio::shm {

#if defined(_WIN32)
template<typename Policy>
using shm_notifier = event_notifier<Policy>;
#elif defined(__APPLE__)
template<typename Policy>
using shm_notifier = sem_notifier<Policy>;
#else
template<typename Policy>
using shm_notifier = ring_notifier<Policy>;
#endif

}

#endif
