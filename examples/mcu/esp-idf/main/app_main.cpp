// Minimal compile-and-link proof of the FreeRTOS Policy substrate against real
// FreeRTOS. Instantiating the executor + timer fires the static_assert(Policy<
// freertos_policy>) in freertos_policy.h under the device toolchain — a clean
// cross-build IS the on-target seam proof. NO transport, NO pin read (that is a
// later milestone). app_main IS the super-loop (never returns): the user's one
// task drives the executor, with no background plexus thread.

#include "plexus/mcu/freertos_policy.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

extern "C" void app_main()
{
    plexus::mcu::freertos_executor ex;
    plexus::mcu::freertos_timer    t(ex); // proves the executor->timer ctor seam binds on-target

    for(;;)
    {
        // Drain ALL ready work (posted callbacks, the queue, due timers) — the
        // step()/poll() analog. pump() returns false at quiescence; it never blocks.
        while(ex.pump())
        {
        }

        // QUIESCENT: block-with-timeout to yield to the FreeRTOS idle task and feed
        // the task watchdog — never a busy-poll (the host's poll-budget lever is fatal
        // here, it starves the idle task into a watchdog reset). The timeout bounds the
        // wait so armed timers still fire on schedule. Production wakes on the executor
        // queue: xQueueReceive(q, &item, pdMS_TO_TICKS(tick)) / ulTaskNotifyTake(timeout).
        vTaskDelay(pdMS_TO_TICKS(10));
    }
}
