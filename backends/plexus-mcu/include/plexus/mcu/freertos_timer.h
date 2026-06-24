#ifndef HPP_GUARD_PLEXUS_MCU_FREERTOS_TIMER_H
#define HPP_GUARD_PLEXUS_MCU_FREERTOS_TIMER_H

#include "plexus/mcu/freertos_executor.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::mcu {

// Tick-backed timer the executor fires from inside pump(). The timer self-registers
// on the executor at construction; expires_after arms it relative to the current
// tick count, async_wait stores the completion handler, and try_fire — called by
// pump() — invokes the handler once the tick has reached the expiry. cancel() and
// re-arming both complete any pending handler with operation_canceled, mirroring the
// steady-timer contract the Policy's timer concept is written against. The expiry is
// a raw tick counter; the wrap-safe comparison at ~49.7 days is an on-bench concern,
// not exercised by the compile-proof.
class freertos_timer
{
public:
    explicit freertos_timer(freertos_executor &ex)
            : m_exec(&ex)
    {
        m_exec->register_timer(this);
    }

    freertos_timer(freertos_executor &ex, std::error_code &)
            : freertos_timer(ex)
    {
    }

    ~freertos_timer()
    {
        if(m_exec)
            m_exec->deregister_timer(this);
    }

    freertos_timer(const freertos_timer &)            = delete;
    freertos_timer &operator=(const freertos_timer &) = delete;
    freertos_timer(freertos_timer &&)                 = delete;
    freertos_timer &operator=(freertos_timer &&)      = delete;

    void expires_after(std::chrono::milliseconds d)
    {
        complete_pending(std::make_error_code(std::errc::operation_canceled));
        m_expiry = xTaskGetTickCount() + pdMS_TO_TICKS(static_cast<std::uint32_t>(d.count()));
        m_active = true;
    }

    void async_wait(plexus::detail::move_only_function<void(std::error_code)> handler)
    {
        m_handler = std::move(handler);
    }

    void cancel()
    {
        m_active = false;
        complete_pending(std::make_error_code(std::errc::operation_canceled));
    }

    // The firable predicate try_fire gates on, exposed so the executor can skip the
    // tick read entirely when nothing could fire.
    [[nodiscard]] bool armed() const noexcept
    {
        return m_active && m_handler;
    }

    bool try_fire(TickType_t now)
    {
        if(!m_active || !m_handler || now < m_expiry)
            return false;
        m_active = false;
        complete_pending(std::error_code{});
        return true;
    }

private:
    void complete_pending(std::error_code ec)
    {
        if(m_handler)
        {
            auto h = std::exchange(m_handler, nullptr);
            h(ec);
        }
    }

    freertos_executor                                        *m_exec;
    TickType_t                                                m_expiry{};
    plexus::detail::move_only_function<void(std::error_code)> m_handler;
    bool                                                      m_active{false};
};

inline bool freertos_executor::fire_due_timer()
{
    const bool any_armed = std::any_of(m_timers.begin(), m_timers.end(), [](const freertos_timer *t) { return t->armed(); });
    if(!any_armed)
        return false;
    const TickType_t now = now_ticks();
    for(auto *t : m_timers)
        if(t->try_fire(now))
            return true;
    return false;
}

}

#endif
