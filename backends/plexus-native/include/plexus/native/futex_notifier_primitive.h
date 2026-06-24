#ifndef HPP_GUARD_PLEXUS_NATIVE_FUTEX_NOTIFIER_PRIMITIVE_H
#define HPP_GUARD_PLEXUS_NATIVE_FUTEX_NOTIFIER_PRIMITIVE_H

#include "plexus/shm/ring_layout.h"

#include <atomic>
#include <cstdint>

namespace plexus::native {

// The wake decision for the gated overload: a FUTEX_WAKE is issued only when the
// park-state word the consumer published was PARKED. EMPTY means the consumer is
// spinning (no syscall needed); NOTIFIED means a wake is already pending (the
// consumer will observe the generation move and re-drain without a second
// syscall). Pure predicate so the gating logic is unit-assertable off the syscall.
[[nodiscard]] inline bool should_wake(std::uint32_t prior_park_state) noexcept
{
    return prior_park_state == ::plexus::shm::k_park_parked;
}

// The irreducible cross-process wakeup primitives: the raw by-address futex
// signal/wait pair the ring's notify_generation word rides on, plus the raw
// eventfd the asio reactor bridge edge-triggers. These are the syscall bodies
// (mechanism), so they live in the compiled backend, NOT the header-only core --
// no core translation unit ever pulls the kernel futex header. The asio reactor
// bridge composes these behind the core notifier concept; this header exposes
// only the bare primitives.

// Bump the shared generation word (release, so a waiter that wakes observes every
// prior payload write) THEN wake every waiter blocked on its address. The wakeup
// crosses process boundaries on a word in a MAP_SHARED region: it keys on the
// physical page, NOT a process-local table, so the syscall uses the SHARED futex
// variant (no private-flag bit) and NEVER std::atomic::wait/notify (the libstdc++
// waiter table is process-local and elides
// the cross-process wake -- host-verified broken). Called on the publish path
// after a slot commits.
void notifier_signal(std::atomic<std::uint32_t> &generation) noexcept;

// The parked-flag-gated wake (the same generation bump as the one-arg form, then a
// FUTEX_WAKE ONLY when a consumer was parked). It swaps the park-state word to
// NOTIFIED and issues the syscall solely when the prior state was PARKED, so a
// busy/spinning (EMPTY) consumer costs the producer zero wake syscalls while a
// parked consumer is still woken. The consumer's notifier stores PARKED (release)
// before committing to the futex wait and re-checks the generation (drain-before-
// wait), which rules out the lost wakeup the one-arg always-wake form avoids by
// waking unconditionally. The one-arg overload is retained UNCHANGED for the call
// sites that have no park-state word.
void notifier_signal(std::atomic<std::uint32_t> &generation,
                     std::atomic<std::uint32_t> &park_state) noexcept;

// Block on the generation word until it moves off `last_seen` (or a spurious
// wake). The drain-before-wait protocol: a consumer reads the word, drains, then
// waits on the value it last saw, so a bump that lands between the drain and the
// wait is never lost (the word already moved past last_seen and the wait returns
// immediately).
void notifier_wait(std::atomic<std::uint32_t> &generation, std::uint32_t last_seen) noexcept;

// A raw, non-blocking eventfd (the fallback-bridge edge source: the futex-watcher
// writes it, the asio reactor reads it). Returns the fd, or -1 on failure (errno
// set). The caller owns the fd lifetime (close()).
int make_notifier_eventfd() noexcept;

}

#endif
