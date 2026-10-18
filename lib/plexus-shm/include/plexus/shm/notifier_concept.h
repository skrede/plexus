#ifndef HPP_GUARD_PLEXUS_SHM_NOTIFIER_CONCEPT_H
#define HPP_GUARD_PLEXUS_SHM_NOTIFIER_CONCEPT_H

#include "plexus/shm/arm_status.h"

#include "plexus/detail/compat.h"

#include <cstdint>
#include <utility>
#include <concepts>

namespace plexus::shm {

// The cross-process wakeup seam, defined in core so no core translation unit pulls
// a kernel futex or asio header. disarm() must run BEFORE the subscribers the drain
// touches are destroyed -- the non-negotiable teardown ordering the registry enforces.
// The arm slot is the waiter's registered cursor index in the ring's control header.
template<typename T>
concept notifier = requires(T &n, std::uint32_t slot, plexus::detail::move_only_function<void()> drain) {
    { n.signal() } -> std::same_as<void>;
    { n.arm(slot, std::move(drain)) } -> std::same_as<arm_status>;
    { n.disarm() } -> std::same_as<void>;
};

// A notifier that can learn after arming that it cannot serve its waiter: it runs the injected demotion with the reason
// on the executor, after the handler that discovered it has returned.
template<typename T>
concept demotable_notifier = requires(T &n, std::uint32_t slot, plexus::detail::move_only_function<void()> drain, plexus::detail::move_only_function<void(arm_status)> demote) {
    { n.signal() } -> std::same_as<void>;
    { n.arm(slot, std::move(drain), std::move(demote)) } -> std::same_as<arm_status>;
    { n.disarm() } -> std::same_as<void>;
};

// A demotable notifier that also reports, with the primary's reason, each time it serves its waiter from a lesser wake
// than the one it prefers.
template<typename T>
concept demotion_reporting_notifier = demotable_notifier<T> &&
        requires(T &n, std::uint32_t slot, plexus::detail::move_only_function<void()> drain, plexus::detail::move_only_function<void(arm_status)> lost,
                 plexus::detail::move_only_function<void(arm_status)> demoted) {
            { n.arm(slot, std::move(drain), std::move(lost), std::move(demoted)) } -> std::same_as<arm_status>;
        };

}

#endif
