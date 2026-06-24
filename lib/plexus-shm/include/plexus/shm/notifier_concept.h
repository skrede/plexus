#ifndef HPP_GUARD_PLEXUS_SHM_NOTIFIER_CONCEPT_H
#define HPP_GUARD_PLEXUS_SHM_NOTIFIER_CONCEPT_H

#include "plexus/detail/compat.h"

#include <concepts>

namespace plexus::shm {

// The cross-process wakeup seam, defined in core so no core translation unit pulls
// a kernel futex or asio header. disarm() must run BEFORE the subscribers the drain
// touches are destroyed -- the non-negotiable teardown ordering the registry enforces.
template<typename T>
concept notifier = requires(T &n, plexus::detail::move_only_function<void()> drain) {
    { n.signal() } -> std::same_as<void>;
    n.arm(std::move(drain));
    { n.disarm() } -> std::same_as<void>;
};

}

#endif
