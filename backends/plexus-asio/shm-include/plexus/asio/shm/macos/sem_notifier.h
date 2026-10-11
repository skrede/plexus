#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SEM_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SEM_NOTIFIER_H

#include "plexus/asio/shm/detail/fd_doorbell.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/detail/notifier_bridge.h"

#include "plexus/detail/compat.h"

#include <fcntl.h>
#include <unistd.h>
#include <semaphore.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::asio::shm {

// macOS has no shared-page futex: a named POSIX counting semaphore both peers derive from the
// region name carries the wake. One lifetime-bounded thread off the executor waits on it and pokes
// a process-local self-pipe the user's io_context watches.
template<typename Policy>
class sem_notifier
{
    struct state
    {
        state(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::string sem_name) noexcept
                : sem(SEM_FAILED)
                , owns_sem(false)
                , pipe_write(-1)
                , sem_name(std::move(sem_name))
                , stop(false)
                , doorbell()
                , word(word)
                , bridge(executor)
        {
        }

        sem_t *sem;
        bool owns_sem;
        int pipe_write;
        std::string sem_name;
        std::atomic<bool> stop;
        detail::fd_doorbell doorbell;
        std::atomic<std::uint32_t> &word;
        ::plexus::shm::detail::notifier_bridge<Policy> bridge;
    };

public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    sem_notifier(typename Policy::executor_type executor, ::plexus::shm::control_header_t &header, std::string_view region_name)
            : m_waiter()
            , m_state(std::make_shared<state>(executor, header.notify_generation, sem_name_for(region_name)))
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

    // Ungated: the counting semaphore latches a post made outside the wait window.
    void signal() noexcept
    {
        m_state->word.fetch_add(1, std::memory_order_release);
        if(m_state->sem != SEM_FAILED)
            ::sem_post(m_state->sem);
    }

    ::plexus::shm::arm_status arm(std::uint32_t, drain_fn drain)
    {
        state &s = *m_state;
        s.bridge.arm(std::move(drain));
        if(!open_semaphore(s) || !open_pipe(s))
            return ::plexus::shm::arm_status::exhausted;
        s.doorbell.attach(s.bridge.executor());
        detail::relay_wakes(m_state, s.doorbell, s.bridge);
        s.stop.store(false, std::memory_order_release);
        m_waiter = std::thread([&s] { wait_loop(s); });
        return ::plexus::shm::arm_status::armed;
    }

    // The join precedes the doorbell close so the worker never pokes a torn pipe.
    void disarm() noexcept
    {
        state &s = *m_state;
        s.bridge.disarm();
        if(m_waiter.joinable())
        {
            s.stop.store(true, std::memory_order_release);
            if(s.sem != SEM_FAILED)
                ::sem_post(s.sem);
            m_waiter.join();
        }
        s.doorbell.close();
        if(s.pipe_write >= 0)
            ::close(s.pipe_write);
        s.pipe_write = -1;
        close_semaphore(s);
    }

private:
    // "/pxn." (5) + the fixed 16-hex region name = 21 chars, within the macOS 31-char name budget.
    static std::string sem_name_for(std::string_view region_name)
    {
        std::string name = "/pxn.";
        name.append(region_name);
        return name;
    }

    // O_EXCL settles ownership: the creator unlinks the name on teardown, an attacher only closes.
    static bool open_semaphore(state &s) noexcept
    {
        s.sem = ::sem_open(s.sem_name.c_str(), O_CREAT | O_EXCL, 0600, 0);
        if(s.sem != SEM_FAILED)
        {
            s.owns_sem = true;
            return true;
        }
        s.sem = ::sem_open(s.sem_name.c_str(), O_CREAT, 0600, 0);
        return s.sem != SEM_FAILED;
    }

    static void close_semaphore(state &s) noexcept
    {
        if(s.sem == SEM_FAILED)
            return;
        ::sem_close(s.sem);
        if(s.owns_sem)
            ::sem_unlink(s.sem_name.c_str());
        s.sem = SEM_FAILED;
    }

    static bool open_pipe(state &s) noexcept
    {
        int fds[2] = {-1, -1};
        if(::pipe(fds) != 0)
            return false;
        ::fcntl(fds[0], F_SETFL, O_NONBLOCK);
        ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);
        ::fcntl(fds[1], F_SETFD, FD_CLOEXEC);
        s.doorbell.adopt(fds[0], detail::fd_doorbell::kind::bytes);
        s.pipe_write = fds[1];
        return true;
    }

    // Drain-before-wait: a post between the poke and the sem_wait is latched by the count.
    static void wait_loop(state &s) noexcept
    {
        while(!s.stop.load(std::memory_order_acquire))
        {
            const unsigned char one = 1;
            (void)::write(s.pipe_write, &one, sizeof(one));
            ::sem_wait(s.sem);
        }
    }

    std::thread m_waiter;
    std::shared_ptr<state> m_state;
};

}

#endif
