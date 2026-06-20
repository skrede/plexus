#include "support/xproc_harness.h"

#include <asio/io_context.hpp>
#include <asio/posix/stream_descriptor.hpp>

#include <liburing.h>

#include <catch2/catch_test_macros.hpp>

#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <system_error>

// The thread-free notifier spike (NON-GATING). It answers ONE question
// with host evidence: can an IORING_OP_FUTEX_WAIT completion on a MAP_SHARED
// word be reaped THROUGH the user's asio reactor WITHOUT a plexus-spawned
// thread? The integration is genuinely unproven on this host because the
// vendored asio is on the epoll backend (no ASIO_HAS_IO_URING), so asio will not
// natively drive io_uring — the only seam is io_uring_register_eventfd ->
// asio::posix::stream_descriptor::async_wait, which this spike exercises end to
// end. The result is recorded as evidence; it does NOT gate anything
// (the bounded-thread + eventfd fallback is the proven floor).

namespace {

// Raw cross-address-space FUTEX_WAKE — no FUTEX_PRIVATE_FLAG, so the wake
// crosses the MAP_SHARED page into the waiting process (the cross-process wake;
// std::atomic::notify is process-local-table-broken here).
long futex_wake_shared(std::uint32_t *word, int count)
{
    return ::syscall(SYS_futex, word, FUTEX_WAKE, count, nullptr, nullptr, 0);
}

}

TEST_CASE("spike: io_uring futex-wait CQE reaped through the asio reactor", "[shm][spike]")
{
    // A MAP_SHARED futex word visible to a forked child across address spaces.
    auto *word = static_cast<std::uint32_t *>(::mmap(nullptr, sizeof(std::uint32_t),
                                                     PROT_READ | PROT_WRITE,
                                                     MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    REQUIRE(word != MAP_FAILED);
    *word = 0;

    io_uring  ring;
    const int ring_rc = ::io_uring_queue_init(8, &ring, 0);
    REQUIRE(ring_rc == 0);

    // The ONLY asio seam available on the epoll-backed asio: register an eventfd
    // that the io_uring kernel side signals on every CQE, and let asio's reactor
    // wait on it. A readable eventfd means "a CQE is pending" — the reactor turn
    // then reaps it with NO plexus thread.
    const int evfd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    REQUIRE(evfd >= 0);
    REQUIRE(::io_uring_register_eventfd(&ring, evfd) == 0);

    // Arm a futex-wait SQE: complete when *word changes away from 0.
    io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    REQUIRE(sqe != nullptr);
    ::io_uring_prep_futex_wait(sqe, word, /*val=*/0, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
    REQUIRE(::io_uring_submit(&ring) == 1);

    // Fork: the child bumps the word and FUTEX_WAKEs it; the parent drives its
    // asio reactor and asserts the CQE surfaces via the registered eventfd.
    bool cqe_seen_in_reactor = false;

    const pid_t pid = ::fork();
    if(pid == 0)
    {
        // Give the parent a beat to arm before the wake (a lost wake would just
        // make the spike inconclusive, not wrong — the parent has a timeout).
        const timespec nap{0, 50 * 1000 * 1000};
        ::nanosleep(&nap, nullptr);
        __atomic_store_n(word, 1u, __ATOMIC_RELEASE);
        futex_wake_shared(word, 1);
        ::_exit(0);
    }
    REQUIRE(pid > 0);

    asio::io_context               io;
    asio::posix::stream_descriptor evt(io, evfd);

    bool wait_fired = false;
    evt.async_wait(asio::posix::stream_descriptor::wait_read,
                   [&](const std::error_code &ec)
                   {
                       wait_fired            = !ec;
                       std::uint64_t drained = 0;
                       (void)::read(evfd, &drained, sizeof(drained));
                       // The eventfd fired — a CQE is pending. Reap it on the
                       // reactor turn (no thread) and confirm it is the futex
                       // completion (res >= 0).
                       io_uring_cqe *cqe = nullptr;
                       if(::io_uring_peek_cqe(&ring, &cqe) == 0 && cqe != nullptr)
                       {
                           cqe_seen_in_reactor = cqe->res >= 0;
                           ::io_uring_cqe_seen(&ring, cqe);
                       }
                   });

    // Bound the reactor so a missed wake cannot hang the suite.
    io.run_for(std::chrono::seconds(2));

    int status = 0;
    ::waitpid(pid, &status, 0);
    evt.release();
    ::close(evfd);
    ::io_uring_queue_exit(&ring);
    ::munmap(word, sizeof(std::uint32_t));

    // The load-bearing evidence: the eventfd seam fired on the asio
    // reactor AND the futex CQE was reaped there, with NO plexus thread. If
    // either is false the bounded-thread fallback is the practical primary.
    INFO("eventfd async_wait fired on reactor: " << wait_fired);
    INFO("futex CQE reaped in reactor turn: " << cqe_seen_in_reactor);
    REQUIRE(wait_fired);
    REQUIRE(cqe_seen_in_reactor);
}
