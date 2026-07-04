#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SHM_NOTIFIER_SELECT_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SHM_NOTIFIER_SELECT_H

#include "plexus/asio/shm/macos/sem_notifier.h"

namespace plexus::asio::shm {

template<typename P>
using mac_notifier = sem_notifier<P>;

}

#endif
