#include "plexus/native/futex_notifier_primitive.h"

#include <climits>

#include <unistd.h>
#include <linux/futex.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>

namespace plexus::native {

namespace {

// The raw by-address futex syscall. NO private-flag bit ORed into `op`: the
// wakeup must cross process boundaries on a word in a MAP_SHARED region, so the
// kernel keys the wait/wake on the physical page, not on a process-local table
// (the private futex variant would key process-locally and the wake would never
// cross address spaces). std::atomic's
// wait/notify is deliberately NOT used here: libstdc++ tracks waiter contention
// in a process-local table and elides the wake when the notifying process sees
// no local waiter -- which a different process always does -- so it is broken
// cross-process (host-verified). The word is a 4-byte lock-free atomic, the
// same width as the kernel futex word, so the reinterpret to a uint32_t* is
// sound.
int futex_op(std::uint32_t *word, int op, std::uint32_t val) noexcept
{
    return static_cast<int>(::syscall(SYS_futex, word, op, val, nullptr, nullptr, 0));
}

}

void notifier_signal(std::atomic<std::uint32_t> &generation) noexcept
{
    generation.fetch_add(1, std::memory_order_release);
    futex_op(reinterpret_cast<std::uint32_t *>(&generation), FUTEX_WAKE, INT_MAX);
}

void notifier_signal(std::atomic<std::uint32_t> &generation,
                     std::atomic<std::uint32_t> &park_state) noexcept
{
    generation.fetch_add(1, std::memory_order_release);
    // Publish NOTIFIED and read the prior state in one swap (acq_rel): the acquire
    // half pairs with the consumer's release store of PARKED, so a PARKED we observe
    // here happened-before our generation bump is visible to it. Wake only when the
    // consumer was actually parked; a spinning (EMPTY) consumer sees the generation
    // move on its own, and an already-NOTIFIED state means a wake is already pending.
    const std::uint32_t prior =
            park_state.exchange(::plexus::io::shm::k_park_notified, std::memory_order_acq_rel);
    if(should_wake(prior))
        futex_op(reinterpret_cast<std::uint32_t *>(&generation), FUTEX_WAKE, INT_MAX);
}

void notifier_wait(std::atomic<std::uint32_t> &generation, std::uint32_t last_seen) noexcept
{
    futex_op(reinterpret_cast<std::uint32_t *>(&generation), FUTEX_WAIT, last_seen);
}

int make_notifier_eventfd() noexcept { return ::eventfd(0, EFD_NONBLOCK); }

}
