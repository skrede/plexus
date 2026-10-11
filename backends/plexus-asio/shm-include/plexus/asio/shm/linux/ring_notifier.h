#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_RING_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_RING_NOTIFIER_H

// The configure probe for io_uring_prep_futex_wait (liburing >= 2.6) sets this; an absent define selects the threaded floor.
#ifndef PLEXUS_HAS_IO_URING_FUTEX
    #define PLEXUS_HAS_IO_URING_FUTEX 0
#endif

#include "plexus/asio/shm/linux/futex_notifier.h"
#if PLEXUS_HAS_IO_URING_FUTEX
    #include "plexus/asio/shm/linux/uring_notifier.h"
#endif

#include "plexus/shm/fallback_notifier.h"

namespace plexus::asio::shm {

#if PLEXUS_HAS_IO_URING_FUTEX
template<typename Policy>
using ring_notifier = ::plexus::shm::fallback_notifier<ring_notifier_uring<Policy>, ring_notifier_threaded<Policy>>;
#else
template<typename Policy>
using ring_notifier = ring_notifier_threaded<Policy>;
#endif

}

#endif
