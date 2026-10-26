#ifndef HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_EXECUTOR_H
#define HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_EXECUTOR_H

#include "plexus/freertos/posted_work.h"

#include "plexus/freertos/detail/freertos_ingress.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/driver_record.h"

#include <deque>
#include <chrono>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::freertos {

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
// A task that does not pump hands work over through post_from_thread; ingress_dropped() counts
// the posts a full slot pool or a full queue turned away.
class freertos_executor
{
    using pump_scope = plexus::detail::driver_record<TaskHandle_t>::step_scope;

public:
    static constexpr std::uint32_t k_queue_depth = 16;

    freertos_executor()
            : m_queue(xQueueCreate(k_queue_depth, sizeof(posted_work)))
            , m_timers()
            , m_ingress()
            , m_driver()
            , m_posted()
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
    // returns false (the work was NOT enqueued) and the producer keeps and releases
    // its own slot — a bounded, measurable drop, not corruption. An ISR producer, if
    // one ever lands, gets its own re-introduced ISR verb at that time.
    bool post_from_task(posted_work w) noexcept
    {
        return xQueueSend(m_queue, &w, 0) == pdTRUE;
    }

    void post_from_thread(plexus::detail::move_only_function<void()> fn)
    {
        m_ingress.post(std::move(fn), [this](posted_work w) { return post_from_task(w); });
    }

    std::uint32_t ingress_dropped() const noexcept
    {
        return m_ingress.dropped();
    }

    bool running_in_this_thread() noexcept
    {
        return m_driver.running_in(xTaskGetCurrentTaskHandle());
    }

    bool inside_executor() const noexcept
    {
        return m_driver.inside(xTaskGetCurrentTaskHandle());
    }

    // The watchdog-safe idle: yield the core until a cross-context post arrives OR the interval
    // elapses, running a post that wakes it. Blocking on the queue (not an unconditional delay) is
    // what lets an RX-task delivery reach the engine immediately instead of waiting out the park —
    // the receive-latency lever for the posted (P2) policy. A poll-only (P1) drive posts nothing, so
    // it parks the full interval exactly as a plain delay would. The queue is depth-bounded, so even
    // under a sustained post burst the loop drains and re-parks rather than spinning the idle out.
    void park(std::chrono::milliseconds interval) noexcept
    {
        posted_work w{};
        if(xQueueReceive(m_queue, &w, pdMS_TO_TICKS(static_cast<std::uint32_t>(interval.count()))) == pdTRUE)
        {
            const pump_scope scope(m_driver, xTaskGetCurrentTaskHandle());
            w.invoke(w.ctx);
        }
    }

    bool pump()
    {
        const pump_scope scope(m_driver, xTaskGetCurrentTaskHandle());
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
    QueueHandle_t m_queue;
    std::vector<freertos_timer *> m_timers;
    detail::freertos_ingress<k_queue_depth> m_ingress;
    plexus::detail::driver_record<TaskHandle_t> m_driver;
    std::deque<plexus::detail::move_only_function<void()>> m_posted;

    // The tick is read only here, and only when some timer could actually fire —
    // never for an armed-but-handlerless or canceled timer. The scan over the
    // (complete) timer type is out-of-line in the timer header.
    static TickType_t now_ticks() noexcept
    {
        return xTaskGetTickCount();
    }

    bool fire_due_timer();
};

}

#endif
