#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_RING_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_RING_NOTIFIER_H

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/ring_layout.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/posix/stream_descriptor.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <liburing.h>

#include <unistd.h>
#include <linux/futex.h>

#include <atomic>
#include <memory>
#include <cstdint>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>

namespace plexus::asio::shm {

// The wakeup -> reactor bridge: lands a cross-process futex wake in the user's asio reactor turn
// without plexus owning a hidden loop. A private io_uring submits IORING_OP_FUTEX_WAIT on the
// ring's in-region notify-generation word; a registered eventfd is the completion doorbell, watched
// on the user's io_context. Each wake reaps the futex completion, POSTS the drain onto the user's
// executor (Policy::post, never a serialized wrapper), and re-submits the wait. No plexus-spawned
// thread. ring_notifier_threaded (below) carries the same seam where the kernel lacks io_uring
// futex-wait. Non-copy/non-move: the io_uring, the eventfd, and the asio descriptor hold stable
// addresses and the in-flight handler captures `this`.
template<typename Policy>
class ring_notifier
{
public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    ring_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park) noexcept
            : m_executor(executor)
            , m_word(word)
            , m_park(park)
    {
    }

    // A single-process harness whose producer drives the always-wake one-arg signal never reads
    // this consumer's park word, so it binds an owned fallback atom rather than an in-region word.
    ring_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word) noexcept
            : m_executor(executor)
            , m_word(word)
            , m_park(m_park_fallback)
    {
    }

    ~ring_notifier()
    {
        disarm();
    }

    ring_notifier(const ring_notifier &)            = delete;
    ring_notifier &operator=(const ring_notifier &) = delete;
    ring_notifier(ring_notifier &&)                 = delete;
    ring_notifier &operator=(ring_notifier &&)      = delete;

    // The gated primitive skips the FUTEX_WAKE syscall when no waiter is parked (a spinning
    // consumer costs the producer zero wakes).
    void signal() noexcept
    {
        ::plexus::native::notifier_signal(m_word, m_park);
    }

    void arm(drain_fn drain)
    {
        m_drain = std::move(drain);
        if(::io_uring_queue_init(k_ring_depth, &m_ring, 0) != 0)
            return; // bring-up failure: the seam stays inert (disarm is a no-op)
        m_ring_live = true;

        m_evfd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if(m_evfd < 0 || ::io_uring_register_eventfd(&m_ring, m_evfd) != 0)
            return;

        m_doorbell.emplace(executor_io(m_executor), m_evfd);
        submit_futex_wait();
        watch_doorbell();
    }

    // Mandated teardown order: clear the alive token, cancel the asio wait, tear down the io_uring
    // (drops the in-flight futex SQE), then close the eventfd. The asio cancel does NOT un-post a
    // drain a prior on_doorbell already handed to Policy::post — that closure runs on a later poll,
    // after this notifier may be freed; the alive token (checked by value in the posted closure)
    // makes that trailing post a no-op without dereferencing freed `this`.
    void disarm() noexcept
    {
        m_alive->store(false, std::memory_order_release);
        if(m_doorbell)
        {
            std::error_code ignore;
            m_doorbell->cancel(ignore);
            m_doorbell->release(); // detach the fd from asio (we close it ourselves)
            m_doorbell.reset();
        }
        if(m_ring_live)
        {
            ::io_uring_queue_exit(&m_ring);
            m_ring_live = false;
        }
        if(m_evfd >= 0)
        {
            ::close(m_evfd);
            m_evfd = -1;
        }
    }

private:
    static constexpr unsigned k_ring_depth = 8;

    static ::asio::io_context &executor_io(typename Policy::executor_type ex) noexcept
    {
        return ex;
    }

    // FUTEX_BITSET_MATCH_ANY + FUTEX2_SIZE_U32 match the SHARED 32-bit word the producer FUTEX_WAKEs.
    void submit_futex_wait() noexcept
    {
        io_uring_sqe *sqe = ::io_uring_get_sqe(&m_ring);
        if(sqe == nullptr)
            return;
        // Announce the park (release) BEFORE the submit registers the waiter: a producer that bumps
        // the word then reads park_state after the submit observes PARKED and wakes us; a store
        // after the submit would leave a wake-skip window. The snapshot is taken AFTER the park
        // store, so a publish that already advanced the word past it completes the CQE immediately
        // (the kernel value-compares *word != cur on registration) — no lost wake, no stale block.
        m_park.store(::plexus::shm::k_park_parked, std::memory_order_release);
        const std::uint32_t cur = m_word.load(std::memory_order_acquire);
        ::io_uring_prep_futex_wait(sqe, reinterpret_cast<std::uint32_t *>(&m_word), cur, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
        ::io_uring_submit(&m_ring);
    }

    void watch_doorbell() noexcept
    {
        m_doorbell->async_wait(::asio::posix::stream_descriptor::wait_read, [this](const std::error_code &ec) { on_doorbell(ec); });
    }

    void on_doorbell(const std::error_code &ec) noexcept
    {
        if(ec || !m_doorbell)
            return; // aborted on disarm, or already torn down: touch nothing
        std::uint64_t drained = 0;
        (void)::read(m_evfd, &drained, sizeof(drained));
        reap_cqe();
        // Clear the park before re-arming so a producer that bumps the word while the drain is in
        // flight is not gated off a stale PARKED (submit_futex_wait re-stores PARKED before it
        // re-registers the waiter).
        m_park.store(::plexus::shm::k_park_empty, std::memory_order_release);
        Policy::post(m_executor, drain_post());
        submit_futex_wait();
        watch_doorbell();
    }

    // The generation counter coalesces a flood, so one drain pass after reaping every CQE suffices.
    void reap_cqe() noexcept
    {
        io_uring_cqe *cqe = nullptr;
        while(::io_uring_peek_cqe(&m_ring, &cqe) == 0 && cqe != nullptr)
            ::io_uring_cqe_seen(&m_ring, cqe);
    }

    // The closure captures the alive token by value: a post that outlives a disarm (token went
    // false) no-ops instead of touching freed `this`. The token is a liveness flag, NOT shared
    // ownership (the entry stays the single owner and sequences teardown).
    drain_fn drain_post() noexcept
    {
        return [this, alive = m_alive]
        {
            if(alive->load(std::memory_order_acquire) && m_drain)
                m_drain();
        };
    }

    std::atomic<std::uint32_t> m_park_fallback{::plexus::shm::k_park_empty};
    std::shared_ptr<std::atomic<bool>> m_alive = std::make_shared<std::atomic<bool>>(true);

    typename Policy::executor_type m_executor;
    std::atomic<std::uint32_t> &m_word;
    std::atomic<std::uint32_t> &m_park;
    drain_fn m_drain;

    io_uring m_ring{};
    bool m_ring_live = false;
    int m_evfd       = -1;
    std::optional<::asio::posix::stream_descriptor> m_doorbell;
};

// The portable floor for kernels without io_uring futex-wait: ONE lifetime-bounded thread, OFF the
// executor, notifier_wait()s on the in-region word and writes a process-local eventfd an asio read
// handler drains, then POSTS via Policy::post. It carries the identical notifier seam.
template<typename Policy>
class ring_notifier_threaded
{
public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    ring_notifier_threaded(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park) noexcept
            : m_executor(executor)
            , m_word(word)
            , m_park(park)
    {
    }

    // A single-process harness driving the always-wake one-arg signal binds an owned fallback atom
    // (see ring_notifier).
    ring_notifier_threaded(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word) noexcept
            : m_executor(executor)
            , m_word(word)
            , m_park(m_park_fallback)
    {
    }

    ~ring_notifier_threaded()
    {
        disarm();
    }

    ring_notifier_threaded(const ring_notifier_threaded &)            = delete;
    ring_notifier_threaded &operator=(const ring_notifier_threaded &) = delete;
    ring_notifier_threaded(ring_notifier_threaded &&)                 = delete;
    ring_notifier_threaded &operator=(ring_notifier_threaded &&)      = delete;

    void signal() noexcept
    {
        ::plexus::native::notifier_signal(m_word, m_park);
    }

    void arm(drain_fn drain)
    {
        m_drain = std::move(drain);
        m_evfd  = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if(m_evfd < 0)
            return;
        m_doorbell.emplace(executor_io(m_executor), m_evfd);
        watch_doorbell();
        m_stop.store(false, std::memory_order_release);
        m_waiter = std::thread([this] { wait_loop(); });
    }

    // Mandated teardown order: clear the alive token, stop -> wake the parked thread -> join ->
    // cancel/close the fd. The join BEFORE the fd close guarantees the waiter never writes a closed
    // fd; the alive token makes a drain a prior on_doorbell already posted no-op after free.
    void disarm() noexcept
    {
        m_alive->store(false, std::memory_order_release);
        if(m_waiter.joinable())
        {
            m_stop.store(true, std::memory_order_release);
            ::plexus::native::notifier_signal(m_word); // wake it so the wait returns
            m_waiter.join();
        }
        if(m_doorbell)
        {
            std::error_code ignore;
            m_doorbell->cancel(ignore);
            m_doorbell->release();
            m_doorbell.reset();
        }
        if(m_evfd >= 0)
        {
            ::close(m_evfd);
            m_evfd = -1;
        }
    }

private:
    static ::asio::io_context &executor_io(typename Policy::executor_type ex) noexcept
    {
        return ex;
    }

    // Drain-before-wait: poke the doorbell, then FUTEX_WAIT on the word snapshot — a bump between
    // the snapshot and the wait returns immediately, so no wake is lost.
    void wait_loop() noexcept
    {
        std::uint32_t last_seen = m_word.load(std::memory_order_acquire);
        while(!m_stop.load(std::memory_order_acquire))
        {
            poke_doorbell();
            // Announce the park (release) BEFORE the FUTEX_WAIT so the gated producer observes
            // PARKED and wakes us; FUTEX_WAIT value-compares last_seen, so a publish that already
            // moved the word returns immediately. Clear to EMPTY on return.
            m_park.store(::plexus::shm::k_park_parked, std::memory_order_release);
            ::plexus::native::notifier_wait(m_word, last_seen);
            m_park.store(::plexus::shm::k_park_empty, std::memory_order_release);
            last_seen = m_word.load(std::memory_order_acquire);
        }
    }

    void poke_doorbell() noexcept
    {
        const std::uint64_t one = 1;
        (void)::write(m_evfd, &one, sizeof(one));
    }

    void watch_doorbell() noexcept
    {
        m_doorbell->async_wait(::asio::posix::stream_descriptor::wait_read, [this](const std::error_code &ec) { on_doorbell(ec); });
    }

    void on_doorbell(const std::error_code &ec) noexcept
    {
        if(ec || !m_doorbell)
            return; // aborted on disarm
        std::uint64_t drained = 0;
        (void)::read(m_evfd, &drained, sizeof(drained));
        Policy::post(m_executor, drain_post());
        watch_doorbell();
    }

    drain_fn drain_post() noexcept
    {
        return [this, alive = m_alive]
        {
            if(alive->load(std::memory_order_acquire) && m_drain)
                m_drain();
        };
    }

    std::atomic<std::uint32_t> m_park_fallback{::plexus::shm::k_park_empty};
    std::shared_ptr<std::atomic<bool>> m_alive = std::make_shared<std::atomic<bool>>(true);

    typename Policy::executor_type m_executor;
    std::atomic<std::uint32_t> &m_word;
    std::atomic<std::uint32_t> &m_park;
    drain_fn m_drain;

    std::atomic<bool> m_stop{false};
    std::thread m_waiter;
    int m_evfd = -1;
    std::optional<::asio::posix::stream_descriptor> m_doorbell;
};

}

#endif
