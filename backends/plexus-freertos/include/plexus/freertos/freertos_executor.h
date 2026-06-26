#ifndef HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_EXECUTOR_H
#define HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_EXECUTOR_H

#include "plexus/freertos/detail/freertos_host_shim.h"

#include "plexus/detail/compat.h"

#include <deque>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::freertos {

class freertos_timer;

// A unit of cross-context work carried by value through the executor queue: a
// trivially-copyable 16-byte POD, the task-context analog of the bare pointer the
// queue carried before. The producer owns the storage `ctx` points at; `invoke(ctx)`
// runs the work on the executor task AND releases that storage back to the producer's
// pool — the executor never owns or frees the bytes, so the post path allocates nothing.
struct posted_work
{
    void (*invoke)(void *) noexcept;
    void *ctx;
};

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
            : m_queue(xQueueCreate(k_queue_depth, sizeof(posted_work)))
    {
    }

    freertos_executor(const freertos_executor &)            = delete;
    freertos_executor &operator=(const freertos_executor &) = delete;
    freertos_executor(freertos_executor &&)                 = delete;
    freertos_executor &operator=(freertos_executor &&)      = delete;

    ~freertos_executor()
    {
        vQueueDelete(m_queue);
    }

    void post(plexus::detail::move_only_function<void()> fn)
    {
        m_posted.push_back(std::move(fn));
    }

    // The one cross-context-safe ingress: a second task hands work to the cooperative
    // loop through the FreeRTOS queue (never the non-thread-safe m_posted deque),
    // carrying a caller-owned POD by value — no executor-side allocation. A zero wait
    // means a producing transport task never parks on a full queue; a full queue
    // returns errQUEUE_FULL (the work was NOT enqueued) and the producer keeps and
    // releases its own slot — a bounded, measurable drop, not corruption. An ISR
    // producer, if one ever lands, gets its own re-introduced ISR verb at that time.
    void post_from_task(posted_work w) noexcept
    {
        xQueueSend(m_queue, &w, 0);
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

        posted_work w{};
        if(xQueueReceive(m_queue, &w, 0) == pdTRUE)
        {
            w.invoke(w.ctx);
            return true;
        }

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
