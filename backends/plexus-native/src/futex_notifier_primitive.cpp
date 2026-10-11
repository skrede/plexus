#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/waiter_park.h"

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

void notifier_signal(::plexus::shm::control_header_t &header) noexcept
{
    ::plexus::shm::wake_parked(header, ::plexus::shm::signal_parked(header), notifier_wake);
}

void notifier_wake(std::atomic<std::uint32_t> &word) noexcept
{
    futex_op(reinterpret_cast<std::uint32_t *>(&word), FUTEX_WAKE, INT_MAX);
}

void notifier_wait(std::atomic<std::uint32_t> &word, std::uint32_t expected) noexcept
{
    futex_op(reinterpret_cast<std::uint32_t *>(&word), FUTEX_WAIT, expected);
}

int make_notifier_eventfd() noexcept
{
    return ::eventfd(0, EFD_NONBLOCK);
}

}
