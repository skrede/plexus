#ifndef HPP_GUARD_PLEXUS_NATIVE_FUTEX_NOTIFIER_PRIMITIVE_H
#define HPP_GUARD_PLEXUS_NATIVE_FUTEX_NOTIFIER_PRIMITIVE_H

#include "plexus/shm/ring_layout.h"

#include <atomic>
#include <cstdint>

namespace plexus::native {

// The wakeup crosses processes on a word in a MAP_SHARED region: it keys on the
// physical page, so the syscall uses the SHARED futex variant (no private-flag
// bit) and NEVER std::atomic::wait/notify (the libstdc++ waiter table is
// process-local and elides the cross-process wake -- host-verified broken). The
// generation bump is release so a woken waiter observes every prior payload write.
void notifier_signal(std::atomic<std::uint32_t> &generation) noexcept;

// Bumps the ring's generation and wakes only the cursor slots whose waiter was
// PARKED, each on its own park word, so spinning waiters cost no wake syscall.
void notifier_signal(::plexus::shm::control_header_t &header) noexcept;

void notifier_wake(std::atomic<std::uint32_t> &word) noexcept;

// Blocks while the word still reads `expected`; a word that already moved returns
// at once (the kernel compares the value at registration).
void notifier_wait(std::atomic<std::uint32_t> &word, std::uint32_t expected) noexcept;

// Returns the fd, or -1 on failure (errno set). The caller owns the fd lifetime.
int make_notifier_eventfd() noexcept;

}

#endif
