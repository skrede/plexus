#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_RACE_TIMER_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_RACE_TIMER_H

#include "plexus/detail/compat.h"

#include <deque>
#include <chrono>
#include <utility>
#include <system_error>

namespace plexus::testing {

class race_timer;

// One FIFO queue holds posted work and manufactured expiries alike, so a completion already queued outlives its
// timer and runs in order, as on a real single-threaded executor.
class race_executor
{
public:
    race_executor()
            : m_timer(nullptr)
            , m_queue()
    {
    }

    race_executor(const race_executor &)            = delete;
    race_executor &operator=(const race_executor &) = delete;

    void register_timer(race_timer &t) noexcept
    {
        m_timer = &t;
    }

    race_timer &timer() noexcept
    {
        return *m_timer;
    }

    void post(plexus::detail::move_only_function<void()> fn)
    {
        m_queue.push_back(std::move(fn));
    }

    void run_posted()
    {
        while(!m_queue.empty())
        {
            auto fn = std::move(m_queue.front());
            m_queue.pop_front();
            fn();
        }
    }

private:
    race_timer *m_timer;
    std::deque<plexus::detail::move_only_function<void()>> m_queue;
};

// Cancel and re-arm complete the pending wait once, synchronously, with operation_canceled (the inproc contract).
// manufacture_expiry() hands the pending wait to the executor's queue with success, beyond the reach of cancel(),
// which is the expired-but-undispatched state of an asio steady_timer.
class race_timer
{
public:
    using handler_type = plexus::detail::move_only_function<void(std::error_code)>;

    explicit race_timer(race_executor &ex)
            : m_exec(ex)
            , m_pending()
    {
        ex.register_timer(*this);
    }

    race_timer(race_executor &ex, std::error_code &)
            : race_timer(ex)
    {
    }

    race_timer(const race_timer &)            = delete;
    race_timer &operator=(const race_timer &) = delete;

    void expires_after(std::chrono::milliseconds)
    {
        cancel();
    }

    void async_wait(handler_type handler)
    {
        m_pending = std::move(handler);
    }

    void cancel()
    {
        if(auto h = std::exchange(m_pending, nullptr))
            h(std::make_error_code(std::errc::operation_canceled));
    }

    void manufacture_expiry()
    {
        if(auto h = std::exchange(m_pending, nullptr))
            m_exec.post([h = std::move(h)]() mutable { h(std::error_code{}); });
    }

private:
    race_executor &m_exec;
    handler_type m_pending;
};

}

#endif
