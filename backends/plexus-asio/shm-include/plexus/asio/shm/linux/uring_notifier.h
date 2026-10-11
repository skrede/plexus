#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_URING_NOTIFIER_H

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <liburing.h>

#include <linux/futex.h>
#include <sys/eventfd.h>

#include <atomic>
#include <memory>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::asio::shm {

// Lands a cross-process futex wake in the user's reactor turn without a plexus-owned thread: a
// private io_uring submits IORING_OP_FUTEX_WAIT on the in-region word, and its registered eventfd
// is the doorbell watched on the user's io_context.
template<typename Policy>
class ring_notifier_uring
{
    struct state
    {
        state(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park) noexcept
                : ring()
                , ring_live(false)
                , doorbell()
                , word(word)
                , park(park)
                , bridge(executor)
        {
        }

        io_uring ring;
        bool ring_live;
        detail::fd_doorbell doorbell;
        std::atomic<std::uint32_t> &word;
        std::atomic<std::uint32_t> &park;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    ring_notifier_uring(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park)
            : m_park_fallback(::plexus::shm::k_park_empty)
            , m_state(std::make_shared<state>(executor, word, park))
    {
    }

    // A single-process harness whose producer drives the always-wake one-arg signal never reads
    // this consumer's park word, so it binds an owned fallback atom rather than an in-region word.
    ring_notifier_uring(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word)
            : m_park_fallback(::plexus::shm::k_park_empty)
            , m_state(std::make_shared<state>(executor, word, m_park_fallback))
    {
    }

    ~ring_notifier_uring()
    {
        disarm();
    }

    ring_notifier_uring(const ring_notifier_uring &)            = delete;
    ring_notifier_uring &operator=(const ring_notifier_uring &) = delete;
    ring_notifier_uring(ring_notifier_uring &&)                 = delete;
    ring_notifier_uring &operator=(ring_notifier_uring &&)      = delete;

    // The gated primitive skips the FUTEX_WAKE syscall when no waiter is parked.
    void signal() noexcept
    {
        ::plexus::native::notifier_signal(m_state->word, m_state->park);
    }

    void arm(drain_fn drain)
    {
        state &s = *m_state;
        s.bridge.arm(std::move(drain));
        if(::io_uring_queue_init(k_ring_depth, &s.ring, 0) != 0)
            return;
        s.ring_live = true;
        if(!s.doorbell.adopt(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK), detail::fd_doorbell::kind::counter) || ::io_uring_register_eventfd(&s.ring, s.doorbell.native_handle()) != 0)
            return;
        s.doorbell.attach(s.bridge.executor());
        submit_futex_wait(s);
        watch(m_state);
    }

    // Exiting the io_uring drops the in-flight futex wait; the eventfd it signals closes after it.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        s.doorbell.detach();
        if(s.ring_live)
        {
            ::io_uring_queue_exit(&s.ring);
            s.ring_live = false;
        }
        s.doorbell.close();
    }

private:
    static constexpr unsigned k_ring_depth = 8;

    // PARKED is stored before the snapshot and the submit: a producer that bumps the word after
    // the store sees PARKED and wakes us, and one that bumped it before the snapshot completes the
    // wait at registration (the kernel compares *word against the snapshot). FUTEX_BITSET_MATCH_ANY
    // plus FUTEX2_SIZE_U32 match the SHARED 32-bit word the producer wakes.
    static void submit_futex_wait(state &s) noexcept
    {
        io_uring_sqe *sqe = ::io_uring_get_sqe(&s.ring);
        if(sqe == nullptr)
            return;
        s.park.store(::plexus::shm::k_park_parked, std::memory_order_release);
        const std::uint32_t cur = s.word.load(std::memory_order_acquire);
        ::io_uring_prep_futex_wait(sqe, reinterpret_cast<std::uint32_t *>(&s.word), cur, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
        ::io_uring_submit(&s.ring);
    }

    static void watch(const std::shared_ptr<state> &s)
    {
        s->doorbell.watch([s](const std::error_code &ec) { on_doorbell(s, ec); });
    }

    // The park clears before the re-submit so a bump made while the drain is in flight is not
    // gated off a stale PARKED.
    static void on_doorbell(const std::shared_ptr<state> &s, const std::error_code &ec)
    {
        if(ec || !s->doorbell.attached())
            return;
        s->doorbell.drain();
        reap_cqe(*s);
        s->park.store(::plexus::shm::k_park_empty, std::memory_order_release);
        ::plexus::shm::detail::post_drain(s, s->bridge);
        submit_futex_wait(*s);
        watch(s);
    }

    // The generation counter coalesces a flood, so one drain after reaping every CQE suffices.
    static void reap_cqe(state &s) noexcept
    {
        io_uring_cqe *cqe = nullptr;
        while(::io_uring_peek_cqe(&s.ring, &cqe) == 0 && cqe != nullptr)
            ::io_uring_cqe_seen(&s.ring, cqe);
    }

    std::atomic<std::uint32_t> m_park_fallback;
    std::shared_ptr<state> m_state;
};

}

#endif
