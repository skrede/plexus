#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SEM_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SEM_NOTIFIER_H

#include "plexus/asio/shm/macos/sem_doorbell.h"
#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::asio::shm {

// macOS has no shared-page futex: every waiter owns a named semaphore keyed by its region, cursor slot and the tag it
// stores in its cursor. One lifetime-bounded thread off the executor runs the per-waiter park cycle on that semaphore
// and pokes a process-local self-pipe the user's io_context watches; a producer posts only the semaphores of the
// waiters it moved from PARKED to NOTIFIED.
template<typename Policy>
class sem_notifier
{
    struct state
    {
        state(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header) noexcept
                : slot(0)
                , stop(false)
                , pipe()
                , doorbell()
                , header(header)
                , bridge(executor)
        {
        }

        std::uint32_t slot;
        std::atomic<bool> stop;
        detail::fd_doorbell pipe;
        detail::sem_doorbell doorbell;
        ::plexus::shm::control_header_t &header;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    sem_notifier(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header, std::string_view region_name)
            : m_region(region_name)
            , m_waiter()
            , m_state(std::make_shared<state>(executor, header))
            , m_bells()
    {
    }

    ~sem_notifier()
    {
        disarm();
    }

    sem_notifier(const sem_notifier &)            = delete;
    sem_notifier &operator=(const sem_notifier &) = delete;
    sem_notifier(sem_notifier &&)                 = delete;
    sem_notifier &operator=(sem_notifier &&)      = delete;

    void signal() noexcept
    {
        const std::uint32_t mask = ::plexus::shm::signal_parked(m_state->header);
        if(mask != 0)
            m_bells.ring(m_state->header, m_region, mask);
    }

    ::plexus::shm::arm_status arm(std::uint32_t slot, drain_fn drain)
    {
        state &s = *m_state;
        s.slot   = slot;
        s.bridge.arm(std::move(drain));
        if(open_doorbells(s) && start_waiter(s))
            return ::plexus::shm::arm_status::armed;
        disarm();
        return ::plexus::shm::arm_status::exhausted;
    }

    // The stop wake goes to the waiter's own semaphore, which no other process waits on. The join precedes the pipe
    // close so the worker never pokes a torn pipe.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        if(m_waiter.joinable())
        {
            s.stop.store(true, std::memory_order_seq_cst);
            s.doorbell.post();
            m_waiter.join();
        }
        s.pipe.close();
        if(!s.doorbell.created())
            return;
        ::plexus::shm::park_clear(s.header, s.slot);
        s.header.cursors[s.slot].waiter_tag.store(0, std::memory_order_release);
        s.doorbell.close();
    }

private:
    std::string m_region;
    std::thread m_waiter;
    std::shared_ptr<state> m_state;
    detail::sem_doorbell_cache m_bells;

    // The tag is published before the first park, so a producer that sees the slot PARKED names this semaphore.
    bool open_doorbells(state &s)
    {
        const std::optional<std::uint32_t> tag = s.doorbell.create(m_region, s.slot);
        if(!tag || !s.pipe.open_pipe())
            return false;
        s.header.cursors[s.slot].waiter_tag.store(*tag, std::memory_order_release);
        ::plexus::shm::park_clear(s.header, s.slot);
        s.pipe.attach(s.bridge.executor());
        detail::relay_wakes(m_state, s.pipe, s.bridge);
        s.stop.store(false, std::memory_order_release);
        return true;
    }

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
    // publish either fails the recheck or finds the slot PARKED and posts this waiter's semaphore, whose count
    // latches a post that lands before the wait.
    static void wait_loop(state &s) noexcept
    {
        std::uint32_t snapshot = ::plexus::shm::park_snapshot(s.header);
        while(!s.stop.load(std::memory_order_acquire))
        {
            s.pipe.poke();
            ::plexus::shm::park_store(s.header, s.slot);
            if(::plexus::shm::park_recheck(s.header, snapshot) && !s.stop.load(std::memory_order_seq_cst))
                s.doorbell.wait();
            ::plexus::shm::park_clear(s.header, s.slot);
            snapshot = ::plexus::shm::park_snapshot(s.header);
        }
    }
};

}

#endif
