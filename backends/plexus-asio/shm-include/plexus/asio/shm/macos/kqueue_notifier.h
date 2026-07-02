#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_KQUEUE_NOTIFIER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_KQUEUE_NOTIFIER_H

#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/ring_layout.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/posix/stream_descriptor.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <semaphore.h>
#include <sys/event.h>
#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

namespace plexus::asio::shm {

// The cross-process wakeup -> reactor bridge for macOS using an EVFILT_USER doorbell. The cross-
// process leg is still a named POSIX counting semaphore because kqueue EVFILT_USER is process-local
// (see kqueue(2)/kevent(2): NOTE_TRIGGER can only wake a kqueue in the same process, so it cannot
// wake another process's kqueue). ONE lifetime-bounded thread OFF the executor sem_waits on the
// semaphore, then NOTE_TRIGGERs a private kqueue that asio watches for readability; on readiness
// asio drains one event, POSTS via Policy::post, and re-arms. asio owns and hides its own reactor
// kqueue, so the doorbell needs this second, watchable kqueue fd. Non-copy/non-move: the kqueue,
// the semaphore, and the asio descriptor hold stable addresses and the in-flight handler captures
// `this`.
template<typename Policy>
class kqueue_notifier
{
public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    kqueue_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park, std::string_view region_name)
            : m_executor(executor)
            , m_word(word)
            , m_park(park)
            , m_sem_name(sem_name_for(region_name))
    {
    }

    // A single-process harness binds an owned park fallback and a per-instance private semaphore,
    // since no second process opens it by name.
    kqueue_notifier(typename Policy::executor_type executor, std::atomic<std::uint32_t> &word)
            : m_executor(executor)
            , m_word(word)
            , m_park(m_park_fallback)
            , m_sem_name(private_sem_name())
    {
    }

    ~kqueue_notifier()
    {
        disarm();
    }

    kqueue_notifier(const kqueue_notifier &)            = delete;
    kqueue_notifier &operator=(const kqueue_notifier &) = delete;
    kqueue_notifier(kqueue_notifier &&)                 = delete;
    kqueue_notifier &operator=(kqueue_notifier &&)      = delete;

    // Ungated: a counting semaphore latches a post made outside the wait window, so the next
    // sem_wait returns immediately — no value-compare is needed and no wake is lost.
    void signal() noexcept
    {
        m_word.fetch_add(1, std::memory_order_release);
        if(m_sem != SEM_FAILED)
            ::sem_post(m_sem);
    }

    void arm(drain_fn drain)
    {
        m_drain_cb = std::move(drain);
        if(!open_semaphore())
            return; // bring-up failure: the seam stays inert (disarm is a no-op)
        m_kq = ::kqueue();
        if(m_kq < 0)
            return;
        ::fcntl(m_kq, F_SETFD, FD_CLOEXEC);
        struct kevent reg;
        EV_SET(&reg, k_user_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        if(::kevent(m_kq, &reg, 1, nullptr, 0, nullptr) != 0)
            return;
        m_doorbell.emplace(executor_io(m_executor), m_kq);
        watch_doorbell();
        m_stop.store(false, std::memory_order_release);
        m_waiter = std::thread([this] { wait_loop(); });
    }

    // Mandated teardown order: clear the alive token, stop -> wake the parked worker -> join ->
    // cancel/close the doorbell, then close/unlink the semaphore. The join BEFORE the doorbell
    // close guarantees the worker never triggers a torn kqueue; the alive token makes a drain a
    // prior on_doorbell already posted no-op after free.
    void disarm() noexcept
    {
        m_alive->store(false, std::memory_order_release);
        if(m_waiter.joinable())
        {
            m_stop.store(true, std::memory_order_release);
            if(m_sem != SEM_FAILED)
                ::sem_post(m_sem); // wake it so the wait returns
            m_waiter.join();
        }
        if(m_doorbell)
        {
            std::error_code ignore;
            m_doorbell->cancel(ignore);
            m_doorbell->release();
            m_doorbell.reset();
        }
        close_kqueue();
        close_semaphore();
    }

private:
    static constexpr std::uintptr_t k_user_ident = 1;

    static ::asio::io_context &executor_io(typename Policy::executor_type ex) noexcept
    {
        return ex;
    }

    // "/pxn." (5) + the fixed 16-hex region name = 21 chars, within the macOS 31-char name budget.
    static std::string sem_name_for(std::string_view region_name)
    {
        std::string name = "/pxn.";
        name.append(region_name);
        return name;
    }

    static std::string private_sem_name()
    {
        static std::atomic<unsigned> counter{0};
        return "/pxn.t." + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
    }

    // Open the named semaphore both peers share (initial count 0). O_EXCL settles ownership: the
    // creator unlinks the name on teardown, an attacher only closes (mirrors the region owner/attach
    // split — a crashed peer would otherwise leak the name).
    bool open_semaphore() noexcept
    {
        m_sem = ::sem_open(m_sem_name.c_str(), O_CREAT | O_EXCL, 0600, 0);
        if(m_sem != SEM_FAILED)
        {
            m_owns_sem = true;
            return true;
        }
        m_sem = ::sem_open(m_sem_name.c_str(), O_CREAT, 0600, 0);
        return m_sem != SEM_FAILED;
    }

    void close_semaphore() noexcept
    {
        if(m_sem == SEM_FAILED)
            return;
        ::sem_close(m_sem);
        if(m_owns_sem)
            ::sem_unlink(m_sem_name.c_str());
        m_sem = SEM_FAILED;
    }

    void close_kqueue() noexcept
    {
        if(m_kq >= 0)
            ::close(m_kq);
        m_kq = -1;
    }

    // Drain-before-wait: poke the doorbell, then block on the semaphore — a post between the poke
    // and the sem_wait is latched by the count, so no wake is lost.
    void wait_loop() noexcept
    {
        while(!m_stop.load(std::memory_order_acquire))
        {
            poke_doorbell();
            m_park.store(::plexus::shm::k_park_parked, std::memory_order_release);
            ::sem_wait(m_sem);
            m_park.store(::plexus::shm::k_park_empty, std::memory_order_release);
        }
    }

    void poke_doorbell() noexcept
    {
        struct kevent trig;
        EV_SET(&trig, k_user_ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
        (void)::kevent(m_kq, &trig, 1, nullptr, 0, nullptr);
    }

    void watch_doorbell() noexcept
    {
        m_doorbell->async_wait(::asio::posix::stream_descriptor::wait_read, [this](const std::error_code &ec) { on_doorbell(ec); });
    }

    void on_doorbell(const std::error_code &ec) noexcept
    {
        if(ec || !m_doorbell)
            return; // aborted on disarm
        struct kevent got;
        const timespec zero{0, 0};
        (void)::kevent(m_kq, nullptr, 0, &got, 1, &zero);
        Policy::post(m_executor, drain_post());
        watch_doorbell();
    }

    drain_fn drain_post() noexcept
    {
        return [this, alive = m_alive]
        {
            if(alive->load(std::memory_order_acquire) && m_drain_cb)
                m_drain_cb();
        };
    }

    std::atomic<std::uint32_t> m_park_fallback{::plexus::shm::k_park_empty};
    std::shared_ptr<std::atomic<bool>> m_alive = std::make_shared<std::atomic<bool>>(true);

    typename Policy::executor_type m_executor;
    std::atomic<std::uint32_t> &m_word;
    std::atomic<std::uint32_t> &m_park;
    drain_fn m_drain_cb;

    std::string m_sem_name;
    sem_t *m_sem    = SEM_FAILED;
    bool m_owns_sem = false;
    std::atomic<bool> m_stop{false};
    std::thread m_waiter;
    int m_kq = -1;
    std::optional<::asio::posix::stream_descriptor> m_doorbell;
};

}

#endif
