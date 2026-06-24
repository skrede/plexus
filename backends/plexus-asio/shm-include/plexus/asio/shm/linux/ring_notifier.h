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

// over-limit: one cohesive futex/eventfd wake protocol; the signal/arm/disarm/park steps share
// the cross-process generation + park-state words and the eventfd/reactor descriptor, so
// splitting them scatters the wake/wait/seq state across files.

// The wakeup -> reactor bridge: the one asio-coupled shared-memory piece. It
// satisfies the core notifier seam (signal / arm(drain) / disarm) and lands a
// cross-process futex wake in the USER'S existing asio reactor turn without
// plexus owning a hidden loop. The wake mechanism swaps behind this seam; the
// contract -- a drain POSTED onto the user's executor on every wake -- does not.
//
// The thread-free io_uring-futex path (this type): a private io_uring submits an
// IORING_OP_FUTEX_WAIT on the ring's in-region notify-generation word; an eventfd
// registered to that ring (io_uring_register_eventfd) is the completion doorbell,
// wrapped in an asio::posix::stream_descriptor watched on the user's io_context.
// On each cross-process wake the reactor turn reaps the futex completion, POSTS
// the drain onto the user's executor (Policy::post -- never a serialized executor
// wrapper), and re-submits the futex wait. There is NO plexus-spawned thread: the
// io_uring kernel side does the waiting, the user's reactor does the reaping.
//
// The bounded-thread fallback (ring_notifier_threaded, below) carries the SAME
// seam for hosts whose kernel lacks io_uring futex-wait (the floor that ships so
// the build is correct either way).
//
// Construction binds the executor + the in-region word; signal() delegates to
// the compiled plexus-native futex primitive (the producer-side wake), so a
// producer in this or any peer process drives the same word the wait rides on.
// Borrows the executor + word BY REFERENCE; non-copy/non-move (the io_uring +
// the registered eventfd + the asio descriptor hold stable addresses, and the
// in-flight completion handler captures `this`).
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

    // Back-compat construction for a single-process harness whose producer drives the
    // always-wake one-arg signal: the gated producer never reads this consumer's park
    // word, so it binds to an owned fallback atom rather than an in-region word.
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

    // The producer wake: bump the shared generation word (release) and wake a parked
    // consumer — the gated primitive skips the FUTEX_WAKE syscall when no waiter is
    // parked (a spinning consumer costs the producer zero wakes).
    void signal() noexcept
    {
        ::plexus::native::notifier_signal(m_word, m_park);
    }

    // Register the consumer drain and begin watching the word. Brings up the
    // private io_uring, registers the CQE-doorbell eventfd onto the user's
    // reactor, and submits the first futex-wait. Each wake re-arms; disarm() ends
    // the watch.
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

    // Stop watching and release the reactor registration, in the mandated order:
    // cancel the asio wait, tear down the io_uring (which drops the in-flight
    // futex SQE), then close the eventfd. After disarm no posted drain or CQE can
    // touch this notifier — the teardown-race proof gate.
    //
    // The asio wait cancel does NOT un-post a drain a PRIOR on_doorbell already
    // handed to Policy::post: that closure sits on the executor queue and runs on
    // the next poll, AFTER this notifier may have been freed. Clearing the alive
    // token (set false here, checked by value in the posted closure) makes that
    // trailing post a no-op without dereferencing freed `this` — the post-after-free
    // gate the asio-cancel alone cannot close.
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

    // The Policy carries the executor as `io_context &`; the asio descriptor binds
    // to that io_context directly. Kept as a one-liner so the executor-shape
    // coupling lives in exactly one place.
    static ::asio::io_context &executor_io(typename Policy::executor_type ex) noexcept
    {
        return ex;
    }

    // Submit one IORING_OP_FUTEX_WAIT on the in-region word: the CQE completes when
    // a peer bumps the word off its current value. FUTEX_BITSET_MATCH_ANY +
    // FUTEX2_SIZE_U32 match the SHARED (cross-process) 32-bit word the producer
    // FUTEX_WAKEs.
    void submit_futex_wait() noexcept
    {
        io_uring_sqe *sqe = ::io_uring_get_sqe(&m_ring);
        if(sqe == nullptr)
            return;
        // Announce the park BEFORE the snapshot + submit registers the waiter: the
        // kernel registers the futex waiter on submit, so a producer that bumps the
        // word and reads park_state after the submit observes PARKED and wakes us; a
        // store after the submit would leave a wake-skip window. The gated producer's
        // acquire-exchange pairs with this release store. The snapshot is taken AFTER
        // the park store so the value the kernel compares is consistent with the
        // window the producer's bump must move off: if a publish raced and already
        // advanced the word past the snapshot, io_uring's FUTEX_WAIT completes the CQE
        // immediately (the kernel value-compares *word != cur on registration), so the
        // reactor turn re-drains and re-submits -- no lost wake, no blocked-on-stale.
        m_park.store(::plexus::shm::k_park_parked, std::memory_order_release);
        const std::uint32_t cur = m_word.load(std::memory_order_acquire);
        ::io_uring_prep_futex_wait(sqe, reinterpret_cast<std::uint32_t *>(&m_word), cur, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32, 0);
        ::io_uring_submit(&m_ring);
    }

    // Arm the asio wait on the CQE doorbell. The handler runs on the user's reactor
    // turn: it drains the eventfd, reaps the futex CQE, POSTS the drain onto the
    // user's executor (Policy::post -- the on_data-posted model, no serialized
    // executor wrapper), then
    // re-submits the futex wait and re-watches. A cancel (disarm) delivers
    // operation_aborted and the chain ends.
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
        // The wake was delivered: clear the park before re-arming so a producer that
        // bumps the word while the drain is in flight is not gated off a stale PARKED
        // (submit_futex_wait re-stores PARKED right before it re-registers the waiter).
        m_park.store(::plexus::shm::k_park_empty, std::memory_order_release);
        Policy::post(m_executor, drain_post());
        submit_futex_wait();
        watch_doorbell();
    }

    // Drop every pending CQE (the futex completion, plus any coalesced doorbell
    // wakes): the generation counter coalesces a flood, so one drain pass suffices.
    void reap_cqe() noexcept
    {
        io_uring_cqe *cqe = nullptr;
        while(::io_uring_peek_cqe(&m_ring, &cqe) == 0 && cqe != nullptr)
            ::io_uring_cqe_seen(&m_ring, cqe);
    }

    // The drain is posted by VALUE-copying the user's drain into a fresh
    // move_only_function each turn, so the long-lived m_drain stays armed across
    // re-submits. The user's drain is the registry's drain-this-channel callback.
    // The closure also captures the alive token by value: a post that outlives a
    // disarm (the token went false) runs the no-op branch instead of touching freed
    // `this`. The token is a tiny liveness flag, NOT shared ownership of the notifier
    // (the entry remains the single owner; the owner sequences teardown).
    drain_fn drain_post() noexcept
    {
        return [this, alive = m_alive]
        {
            if(alive->load(std::memory_order_acquire) && m_drain)
                m_drain();
        };
    }

    std::atomic<std::uint32_t>         m_park_fallback{::plexus::shm::k_park_empty};
    std::shared_ptr<std::atomic<bool>> m_alive = std::make_shared<std::atomic<bool>>(true);

    typename Policy::executor_type m_executor;
    std::atomic<std::uint32_t>    &m_word;
    std::atomic<std::uint32_t>    &m_park;
    drain_fn                       m_drain;

    io_uring                                        m_ring{};
    bool                                            m_ring_live = false;
    int                                             m_evfd      = -1;
    std::optional<::asio::posix::stream_descriptor> m_doorbell;
};

// The portable floor: ONE lifetime-bounded thread, strictly OFF the executor,
// notifier_wait()s on the in-region word (the compiled SHARED-futex primitive)
// and writes a process-local eventfd that an asio::posix::stream_descriptor read
// handler drains. The drain is POSTED via Policy::post (no serialized executor
// wrapper), exactly as the thread-free type. This is the floor for kernels
// without io_uring futex-wait; it carries the identical notifier seam so the
// registry templates on either path unchanged. Teardown is the mandated ordering:
// stop -> wake the parked thread -> join -> close fd, so no thread or posted drain
// ever touches freed state.
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

    // Back-compat construction for a single-process harness whose producer drives the
    // always-wake one-arg signal: it binds to an owned fallback atom (see ring_notifier).
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

    // Teardown order (non-negotiable): stop -> wake the parked thread (a deliberate
    // signal moves the word so its notifier_wait returns) -> join -> cancel/close
    // the fd. The join BEFORE the fd close guarantees the waiter never writes a
    // closed fd; cancelling the asio wait before close drops the in-flight handler.
    void disarm() noexcept
    {
        // Clear the alive token first: a drain already handed to Policy::post by a prior
        // on_doorbell runs on the next poll AFTER this notifier may be freed, so the posted
        // closure (which captures the token by value) must see it false and no-op. The asio
        // cancel below stops only FUTURE waits, not an already-queued post.
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

    // The single bounded thread: drain-before-wait (Pattern 3). Snapshot the word,
    // poke the doorbell (a delivery the reactor will drain), then FUTEX_WAIT on the
    // snapshot — a bump between the snapshot and the wait returns immediately, so no
    // wake is lost. Exits when m_stop is set (disarm's deliberate signal wakes it).
    void wait_loop() noexcept
    {
        std::uint32_t last_seen = m_word.load(std::memory_order_acquire);
        while(!m_stop.load(std::memory_order_acquire))
        {
            poke_doorbell();
            // Announce the park (release) BEFORE the FUTEX_WAIT so the gated producer
            // observes PARKED and wakes us; FUTEX_WAIT value-compares last_seen, so a
            // publish that already moved the word returns immediately (no lost wake).
            // Clear back to EMPTY on return: a producer bumping while the reactor
            // drains is not gated off a stale PARKED.
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

    std::atomic<std::uint32_t>         m_park_fallback{::plexus::shm::k_park_empty};
    std::shared_ptr<std::atomic<bool>> m_alive = std::make_shared<std::atomic<bool>>(true);

    typename Policy::executor_type m_executor;
    std::atomic<std::uint32_t>    &m_word;
    std::atomic<std::uint32_t>    &m_park;
    drain_fn                       m_drain;

    std::atomic<bool>                               m_stop{false};
    std::thread                                     m_waiter;
    int                                             m_evfd = -1;
    std::optional<::asio::posix::stream_descriptor> m_doorbell;
};

}

#endif
