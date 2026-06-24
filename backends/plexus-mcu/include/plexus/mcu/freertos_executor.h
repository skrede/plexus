#ifndef HPP_GUARD_PLEXUS_MCU_FREERTOS_EXECUTOR_H
#define HPP_GUARD_PLEXUS_MCU_FREERTOS_EXECUTOR_H

#include "plexus/mcu/detail/freertos_host_shim.h"

#include "plexus/detail/compat.h"

#include <deque>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::mcu {

class freertos_timer;

// Cooperative single-task pump-executor for the constrained-target super-loop.
// pump() advances the system by one unit of work with a fixed priority — posted
// callbacks, then a single queue drain, then expired timers — and returns false
// only at quiescence; drain() pumps to quiescence. The clock is read only once the
// ready work is exhausted and only when a timer could actually fire, the asio
// reactor discipline. pump() is the NON-BLOCKING drain: it never spins and never
// blocks. The block-with-timeout that yields to the idle task — the discipline that
// keeps the task watchdog fed — lives in the super-loop driver, not here; a spin or
// busy-wait here would starve the idle task and trip the watchdog reset.
class freertos_executor
{
public:
    freertos_executor()
            : m_queue(xQueueCreate(k_queue_depth, sizeof(void *)))
    {
    }

    freertos_executor(const freertos_executor &)            = delete;
    freertos_executor &operator=(const freertos_executor &) = delete;
    freertos_executor(freertos_executor &&)                 = delete;
    freertos_executor &operator=(freertos_executor &&)      = delete;

    void post(plexus::detail::move_only_function<void()> fn)
    {
        m_posted.push_back(std::move(fn));
    }

    // Reserved ISR seam: an interrupt hands work to the cooperative loop via the
    // FreeRTOS queue, the one cross-context edge in the design. The minimal demo
    // uses only same-context post(); this exists so the seam is in place on-target.
    void post_from_isr(void *work) noexcept
    {
        BaseType_t woken = pdFALSE;
        xQueueSendFromISR(m_queue, &work, &woken);
    }

    bool pump()
    {
        if(!m_posted.empty())
        {
            auto fn = std::move(m_posted.front());
            m_posted.pop_front();
            fn();
            return true;
        }

        void *work = nullptr;
        if(xQueueReceive(m_queue, &work, 0) == pdTRUE)
            return true;

        return fire_due_timer();
    }

    void drain()
    {
        while(pump())
        {
        }
    }

    void register_timer(freertos_timer *t)
    {
        if(std::find(m_timers.begin(), m_timers.end(), t) == m_timers.end())
            m_timers.push_back(t);
    }

    void deregister_timer(freertos_timer *t) noexcept
    {
        std::erase(m_timers, t);
    }

private:
    // The tick is read only here, and only when some timer could actually fire —
    // never for an armed-but-handlerless or cancelled timer. The scan over the
    // (complete) timer type is out-of-line in the timer header.
    static TickType_t now_ticks() noexcept
    {
        return xTaskGetTickCount();
    }
    bool fire_due_timer();

    static constexpr std::uint32_t k_queue_depth = 16;

    QueueHandle_t                                          m_queue;
    std::deque<plexus::detail::move_only_function<void()>> m_posted;
    std::vector<freertos_timer *>                          m_timers;
};

}

#endif
