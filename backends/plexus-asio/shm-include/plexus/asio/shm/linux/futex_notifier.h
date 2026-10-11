#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_FUTEX_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_FUTEX_NOTIFIER_H

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <sys/eventfd.h>

#include <atomic>
#include <memory>
#include <thread>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::asio::shm {

// The floor for kernels without io_uring futex-wait: one lifetime-bounded thread off the executor
// futex-waits on its slot's in-region park word and pokes a process-local eventfd the user's io_context watches.
template<typename Policy>
class ring_notifier_threaded
{
    struct state
    {
        state(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header) noexcept
                : slot(0)
                , stop(false)
                , doorbell()
                , header(header)
                , bridge(executor)
        {
        }

        std::uint32_t slot;
        std::atomic<bool> stop;
        detail::fd_doorbell doorbell;
        ::plexus::shm::control_header_t &header;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    ring_notifier_threaded(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header, std::string_view)
            : m_waiter()
            , m_state(std::make_shared<state>(executor, header))
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
        ::plexus::native::notifier_signal(m_state->header);
    }

    ::plexus::shm::arm_status arm(std::uint32_t slot, drain_fn drain)
    {
        state &s = *m_state;
        s.slot   = slot;
        s.bridge.arm(std::move(drain));
        if(!s.doorbell.adopt(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK), detail::fd_doorbell::kind::counter))
            return ::plexus::shm::arm_status::exhausted;
        s.doorbell.attach(s.bridge.executor());
        detail::relay_wakes(m_state, s.doorbell, s.bridge);
        s.stop.store(false, std::memory_order_release);
        return start_waiter(s) ? ::plexus::shm::arm_status::armed : ::plexus::shm::arm_status::exhausted;
    }

    // The join precedes the close so the waiter never pokes a closed eventfd. The stop store and the waiter's stop
    // re-check after its park store are the same Dekker pair as a publish, so the stop is never slept through.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        if(m_waiter.joinable())
        {
            s.stop.store(true, std::memory_order_seq_cst);
            if(::plexus::shm::notify_if_parked(s.header, s.slot))
                ::plexus::native::notifier_wake(::plexus::shm::park_word(s.header, s.slot));
            m_waiter.join();
        }
        s.doorbell.close();
    }

private:
    // A thread the system refuses (std::system_error) is an exhausted resource the caller can fall back from.
    bool start_waiter(state &s)
    {
        try
        {
            m_waiter = std::thread([&s] { wait_loop(s); });
        }
        catch(const std::system_error &)
        {
            return false;
        }
        return true;
    }

    // The snapshot precedes the poke, so the drain it triggers covers every publish up to the snapshot; a later
    // publish either fails the recheck or finds the word PARKED and wakes it.
    static void wait_loop(state &s) noexcept
    {
        std::uint32_t snapshot = ::plexus::shm::park_snapshot(s.header);
        while(!s.stop.load(std::memory_order_acquire))
        {
            s.doorbell.poke();
            ::plexus::shm::park_store(s.header, s.slot);
            if(::plexus::shm::park_recheck(s.header, snapshot) && !s.stop.load(std::memory_order_seq_cst))
                ::plexus::native::notifier_wait(::plexus::shm::park_word(s.header, s.slot), ::plexus::shm::k_park_parked);
            ::plexus::shm::park_clear(s.header, s.slot);
            snapshot = ::plexus::shm::park_snapshot(s.header);
        }
    }

    std::thread m_waiter;
    std::shared_ptr<state> m_state;
};

}

#endif
