#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_FUTEX_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_FUTEX_NOTIFIER_H

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <unistd.h>
#include <sys/eventfd.h>

#include <atomic>
#include <memory>
#include <thread>
#include <cstdint>
#include <utility>

namespace plexus::asio::shm {

// The floor for kernels without io_uring futex-wait: one lifetime-bounded thread off the executor
// futex-waits on the in-region word and pokes a process-local eventfd the user's io_context watches.
template<typename Policy>
class ring_notifier_threaded
{
    struct state
    {
        state(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park) noexcept
                : stop(false)
                , doorbell()
                , word(word)
                , park(park)
                , bridge(executor)
        {
        }

        std::atomic<bool> stop;
        detail::fd_doorbell doorbell;
        std::atomic<std::uint32_t> &word;
        std::atomic<std::uint32_t> &park;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    ring_notifier_threaded(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park)
            : m_park_fallback(::plexus::shm::k_park_empty)
            , m_waiter()
            , m_state(std::make_shared<state>(executor, word, park))
    {
    }

    // A single-process harness driving the always-wake one-arg signal binds an owned fallback atom.
    ring_notifier_threaded(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word)
            : m_park_fallback(::plexus::shm::k_park_empty)
            , m_waiter()
            , m_state(std::make_shared<state>(executor, word, m_park_fallback))
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
        ::plexus::native::notifier_signal(m_state->word, m_state->park);
    }

    void arm(drain_fn drain)
    {
        state &s = *m_state;
        s.bridge.arm(std::move(drain));
        if(!s.doorbell.adopt(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK), detail::fd_doorbell::kind::counter))
            return;
        s.doorbell.attach(s.bridge.executor());
        detail::relay_wakes(m_state, s.doorbell, s.bridge);
        s.stop.store(false, std::memory_order_release);
        m_waiter = std::thread([&s] { wait_loop(s); });
    }

    // The join precedes the close so the waiter never pokes a closed eventfd.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        if(m_waiter.joinable())
        {
            s.stop.store(true, std::memory_order_release);
            ::plexus::native::notifier_signal(s.word);
            m_waiter.join();
        }
        s.doorbell.close();
    }

private:
    // Drain-before-wait: the poke precedes the park, and FUTEX_WAIT compares against last_seen, so
    // a publish that moved the word after the snapshot returns at once and no wake is lost.
    static void wait_loop(state &s) noexcept
    {
        std::uint32_t last_seen = s.word.load(std::memory_order_acquire);
        while(!s.stop.load(std::memory_order_acquire))
        {
            poke(s.doorbell);
            s.park.store(::plexus::shm::k_park_parked, std::memory_order_release);
            ::plexus::native::notifier_wait(s.word, last_seen);
            s.park.store(::plexus::shm::k_park_empty, std::memory_order_release);
            last_seen = s.word.load(std::memory_order_acquire);
        }
    }

    static void poke(const detail::fd_doorbell &doorbell) noexcept
    {
        const std::uint64_t one = 1;
        (void)::write(doorbell.native_handle(), &one, sizeof(one));
    }

    std::atomic<std::uint32_t> m_park_fallback;
    std::thread m_waiter;
    std::shared_ptr<state> m_state;
};

}

#endif
