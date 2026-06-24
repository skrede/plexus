#ifndef HPP_GUARD_PLEXUS_NATIVE_FUTEX_NOTIFIER_PRIMITIVE_H
#define HPP_GUARD_PLEXUS_NATIVE_FUTEX_NOTIFIER_PRIMITIVE_H

#include "plexus/shm/ring_layout.h"

#include <atomic>
#include <cstdint>

namespace plexus::native {

inline bool should_wake(std::uint32_t prior_park_state) noexcept
{
    return prior_park_state == ::plexus::shm::k_park_parked;
}

// The wakeup crosses processes on a word in a MAP_SHARED region: it keys on the
// physical page, so the syscall uses the SHARED futex variant (no private-flag
// bit) and NEVER std::atomic::wait/notify (the libstdc++ waiter table is
// process-local and elides the cross-process wake -- host-verified broken). The
// generation bump is release so a woken waiter observes every prior payload write.
void notifier_signal(std::atomic<std::uint32_t> &generation) noexcept;

// FUTEX_WAKE only when the prior park-state was PARKED, so a spinning (EMPTY)
// consumer costs zero wake syscalls. The consumer stores PARKED (release) before
// committing to the wait and re-checks the generation (drain-before-wait), ruling
// out the lost wakeup the unconditional one-arg form avoids by always waking.
void notifier_signal(std::atomic<std::uint32_t> &generation, std::atomic<std::uint32_t> &park_state) noexcept;

// Drain-before-wait: a consumer reads the word, drains, then waits on the value it
// last saw, so a bump landing between the drain and the wait is never lost (the
// word already moved past last_seen and the wait returns immediately).
void notifier_wait(std::atomic<std::uint32_t> &generation, std::uint32_t last_seen) noexcept;

// Returns the fd, or -1 on failure (errno set). The caller owns the fd lifetime.
int make_notifier_eventfd() noexcept;

}

#endif
