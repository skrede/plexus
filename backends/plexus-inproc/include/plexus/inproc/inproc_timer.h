#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_TIMER_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_TIMER_H

#include "plexus/inproc/inproc_executor.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <utility>
#include <system_error>

namespace plexus::inproc {

// Virtual-clock timer the executor fires from inside step(). cancel() and re-arming both complete
// any pending handler with operation_canceled, mirroring the steady-timer contract the Policy's
// timer concept is written against.
template<typename Clock = std::chrono::steady_clock>
class inproc_timer
{
public:
    explicit inproc_timer(inproc_executor<Clock> &ex)
            : m_exec(&ex)
    {
        m_exec->register_timer(this);
    }

    inproc_timer(inproc_executor<Clock> &ex, std::error_code &)
            : inproc_timer(ex)
    {
    }

    ~inproc_timer()
    {
        if(m_exec)
            m_exec->deregister_timer(this);
    }

    inproc_timer(const inproc_timer &)            = delete;
    inproc_timer &operator=(const inproc_timer &) = delete;
    inproc_timer(inproc_timer &&)                 = delete;
    inproc_timer &operator=(inproc_timer &&)      = delete;

    void expires_after(std::chrono::milliseconds d)
    {
        complete_pending(std::make_error_code(std::errc::operation_canceled));
        m_expiry = Clock::now() + d;
        m_active = true;
    }

    void async_wait(detail::move_only_function<void(std::error_code)> handler)
    {
        m_handler_cb = std::move(handler);
    }

    void cancel()
    {
        m_active = false;
        complete_pending(std::make_error_code(std::errc::operation_canceled));
    }

    // Exposed so the executor can skip the clock read entirely when nothing could fire.
    bool armed() const noexcept
    {
        return m_active && m_handler_cb;
    }

    bool try_fire(typename Clock::time_point now)
    {
        if(!m_active || !m_handler_cb || now < m_expiry)
            return false;
        m_active = false;
        complete_pending(std::error_code{});
        return true;
    }

private:
    void complete_pending(std::error_code ec)
    {
        if(m_handler_cb)
        {
            auto h = std::exchange(m_handler_cb, nullptr);
            h(ec);
        }
    }

    inproc_executor<Clock> *m_exec;
    typename Clock::time_point m_expiry{};
    detail::move_only_function<void(std::error_code)> m_handler_cb;
    bool m_active{false};
};

}

#endif
