#include "plexus/shm/futex_notifier_primitive.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdint>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The cross-process futex notifier: a producer process bumps the shared
// generation word + signals; a consumer process reads the word -> drains -> waits.
// Two load-bearing proofs:
//   (1) the wake crosses ADDRESS SPACES: a FUTEX_WAKE in the producer process
//       wakes a FUTEX_WAIT in the consumer process on the same MAP_SHARED word
//       (this is exactly what NO FUTEX_PRIVATE_FLAG buys -- a private-flagged
//       futex keys on a process-local table and the wake would never cross).
//   (2) the drain-before-wait protocol is lost-wakeup-safe: a publish that lands
//       BETWEEN the consumer's drain and its notifier_wait(last_seen) is delivered
//       -- the word moved past last_seen so the wait returns immediately, never a
//       silent lost publish.
// Looped N>=100 in-body; the ctest binary is re-run >=3 times for reproducibility.

namespace {

// A shared anonymous page holding the futex generation word. Both fork halves map
// the SAME physical page (MAP_SHARED|MAP_ANONYMOUS inherited across fork), so a
// store in one process is visible to the other and the futex keys on it.
struct shared_word
{
    std::atomic<std::uint32_t> generation{0};
    std::atomic<std::uint32_t> child_armed{0}; // the consumer announces it is about to wait
};

shared_word *map_shared_word()
{
    void *p = ::mmap(nullptr, sizeof(shared_word), PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if(p == MAP_FAILED)
        return nullptr;
    return ::new(p) shared_word{};
}

// Leg 1: a child that waits cross-process for the parent's signal. It announces
// arming, then waits until the generation moves off its last_seen. Returns true if
// it observed the producer's bump (never returns false in practice -- if the wake
// is lost it would block forever and the parent's waitpid would hang, surfacing as
// a test timeout, which is itself the failure signal).
bool consumer_wait_for_signal(shared_word *w)
{
    const std::uint32_t last_seen = w->generation.load(std::memory_order_acquire);
    w->child_armed.store(1, std::memory_order_release);
    // Loop over spurious wakes until the word actually advances.
    while(w->generation.load(std::memory_order_acquire) == last_seen)
        plexus::shm::notifier_wait(w->generation, last_seen);
    return true;
}

}

TEST_CASE("shm.notifier_lostwakeup the wake crosses address spaces", "[shm][notifier_lostwakeup]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        shared_word *w = map_shared_word();
        REQUIRE(w != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);
        if(pid == 0)
        {
            const bool ok = consumer_wait_for_signal(w);
            ::_exit(ok ? 0 : 1);
        }

        // Parent (producer): wait until the consumer has armed, then signal. The
        // arm flag closes the obvious race where the parent signals before the
        // child has even started waiting; even so the child's last_seen snapshot +
        // the while-loop guard make the wake delivery correct regardless of timing.
        while(w->child_armed.load(std::memory_order_acquire) == 0)
            ; // spin until armed
        plexus::shm::notifier_signal(w->generation);

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);

        ::munmap(w, sizeof(shared_word));
    }
}

TEST_CASE("shm.notifier_lostwakeup a publish between drain and wait is delivered",
          "[shm][notifier_lostwakeup]")
{
    // The drain-before-wait protocol, exercised against the real syscall. The
    // consumer snapshots last_seen, drains, THEN a publish lands (the producer
    // bumps the word). When the consumer now calls notifier_wait(last_seen) the
    // kernel sees generation != last_seen and the FUTEX_WAIT returns IMMEDIATELY
    // (EAGAIN) rather than blocking on a wake that already happened -- so the
    // interleaved publish is never a lost wakeup. We assert the wait does not
    // block by observing the word is already past last_seen on return.
    shared_word *w = map_shared_word();
    REQUIRE(w != nullptr);

    for(int iter = 0; iter < 100; ++iter)
    {
        const std::uint32_t last_seen = w->generation.load(std::memory_order_acquire);

        // ... consumer "drains" here (no new data yet) ...

        // The publish lands AFTER the drain but BEFORE the wait (the interleave the
        // protocol must survive): bump the generation past last_seen.
        plexus::shm::notifier_signal(w->generation);
        REQUIRE(w->generation.load(std::memory_order_acquire) != last_seen);

        // The consumer now waits on the STALE last_seen. Because the word already
        // moved, the wait returns immediately and the consumer re-drains -- the
        // publish is delivered, not lost. (A correct wait does not block here.)
        plexus::shm::notifier_wait(w->generation, last_seen);

        // On return the word is still past last_seen: the consumer's re-drain loop
        // sees the new generation and delivers the publish.
        REQUIRE(w->generation.load(std::memory_order_acquire) != last_seen);
    }

    ::munmap(w, sizeof(shared_word));
}
