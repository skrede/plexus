#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/ring_layout.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdint>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The parked-flag-gated wake: a producer publishing while the consumer is SPINNING
// (park_state == EMPTY) issues ZERO FUTEX_WAKE syscalls; a producer publishing while
// the consumer IS parked still wakes it (no lost wakeup). Two proofs:
//   (1) the wake DECISION is gated off the prior park-state (a pure predicate, so the
//       zero-wake-when-spinning claim is asserted off the syscall): should_wake is
//       true ONLY for PARKED, false for EMPTY (spinning) and NOTIFIED (already pending).
//   (2) WOKEN-WHEN-PARKED crosses address spaces: a forked child stores PARKED then
//       FUTEX_WAITs on the shared generation word; the parent's two-arg gated signal
//       observes PARKED and delivers the wake. Looped N>=100; the binary is re-run
//       >=3 times for reproducibility.
// The existing test_shm_notifier_lostwakeup is the UNTOUCHED template for the fork'd
// shared-page pattern; this TU adds the park-state word and exercises the gated path.

namespace {

using namespace plexus::shm;

// A shared anonymous page holding the gated wakeup state: the generation word the
// futex rides on + the 3-state park word the producer reads to gate the syscall.
struct shared_state
{
    std::atomic<std::uint32_t> generation{0};
    std::atomic<std::uint32_t> park_state{k_park_empty};
    std::atomic<std::uint32_t> child_armed{0}; // the consumer announces PARKED is stored
};

shared_state *map_shared_state()
{
    void *p = ::mmap(nullptr, sizeof(shared_state), PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if(p == MAP_FAILED)
        return nullptr;
    return ::new(p) shared_state{};
}

// The consumer half: store PARKED (release) before committing to the wait, announce
// armed, then FUTEX_WAIT until the generation moves. Mirrors the notifier park
// boundary (store PARKED before the wait) so the parent's gated signal sees PARKED.
bool consumer_park_and_wait(shared_state *s)
{
    const std::uint32_t last_seen = s->generation.load(std::memory_order_acquire);
    s->park_state.store(k_park_parked, std::memory_order_release);
    s->child_armed.store(1, std::memory_order_release);
    while(s->generation.load(std::memory_order_acquire) == last_seen)
        plexus::native::notifier_wait(s->generation, last_seen);
    return true;
}

}

TEST_CASE("shm.wake_gating the wake decision is gated off the prior park-state",
          "[shm][wake_gating]")
{
    // The zero-wake-when-spinning claim, asserted off the syscall: a producer wakes
    // ONLY when the consumer was PARKED. EMPTY (spinning) and NOTIFIED (a wake is
    // already pending) skip the FUTEX_WAKE entirely.
    REQUIRE(plexus::native::should_wake(k_park_empty) == false);
    REQUIRE(plexus::native::should_wake(k_park_parked) == true);
    REQUIRE(plexus::native::should_wake(k_park_notified) == false);
}

TEST_CASE("shm.wake_gating a spinning consumer leaves the gated signal NOTIFIED and parks nobody",
          "[shm][wake_gating]")
{
    // Single-process: with the consumer SPINNING (park_state == EMPTY) the gated
    // signal bumps the generation and swaps the park word to NOTIFIED without ever
    // FUTEX_WAITing — so the producer's wake is a no-op syscall-wise (the prior state
    // was not PARKED). We assert the protocol's observable state transition: the
    // generation advanced and the park word reads NOTIFIED (a spinning consumer will
    // observe the generation move on its own and reset it on its next park).
    shared_state *s = map_shared_state();
    REQUIRE(s != nullptr);

    for(int iter = 0; iter < 100; ++iter)
    {
        s->generation.store(0, std::memory_order_relaxed);
        s->park_state.store(k_park_empty, std::memory_order_relaxed);

        const std::uint32_t before = s->generation.load(std::memory_order_acquire);
        plexus::native::notifier_signal(s->generation, s->park_state);

        REQUIRE(s->generation.load(std::memory_order_acquire) != before);
        REQUIRE(s->park_state.load(std::memory_order_acquire) == k_park_notified);
    }

    ::munmap(s, sizeof(shared_state));
}

TEST_CASE("shm.wake_gating a parked consumer is woken by the gated signal across address spaces",
          "[shm][wake_gating]")
{
    // WOKEN-WHEN-PARKED (the lost-wakeup guard on the GATED path): a forked child
    // stores PARKED then FUTEX_WAITs; the parent's two-arg gated signal observes the
    // PARKED state and issues the FUTEX_WAKE that crosses the process boundary. If the
    // gate wrongly skipped the wake the child would block forever and waitpid would
    // hang — surfacing as the failure. Looped N>=100.
    for(int iter = 0; iter < 100; ++iter)
    {
        shared_state *s = map_shared_state();
        REQUIRE(s != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);
        if(pid == 0)
        {
            const bool ok = consumer_park_and_wait(s);
            ::_exit(ok ? 0 : 1);
        }

        // Parent (producer): wait until the child has stored PARKED + armed, then drive
        // the GATED signal. The child's release store of PARKED happens-before the
        // parent's acquire-exchange in the gated signal, so the prior state it reads is
        // PARKED and the wake is issued.
        while(s->child_armed.load(std::memory_order_acquire) == 0)
            ; // spin until the child has parked
        plexus::native::notifier_signal(s->generation, s->park_state);

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);

        ::munmap(s, sizeof(shared_state));
    }
}
