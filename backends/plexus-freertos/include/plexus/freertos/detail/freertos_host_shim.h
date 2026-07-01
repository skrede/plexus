#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_FREERTOS_HOST_SHIM_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_FREERTOS_HOST_SHIM_H

// The executor and timer call a handful of FreeRTOS primitives unqualified, the
// way device code does. ESP_PLATFORM is defined by the ESP-IDF build, so on-target
// this header is a thin pass-through to the real kernel, and the same source binds
// to it. Off-target (the host suite) those headers cannot be included — they pull
// in a per-arch port layer — so this provides host stand-ins for exactly the
// primitives the substrate references, at global scope to match the real
// declarations, and nothing more. The stand-ins are host-test scaffolding under
// detail/, never shipped to the device.
#if !defined(ESP_PLATFORM)

    #include <deque>
    #include <vector>
    #include <chrono>
    #include <cstring>
    #include <cstdint>

using TickType_t             = std::uint32_t;
using BaseType_t             = int;
using UBaseType_t            = unsigned int;
using TaskHandle_t           = void *;
using TaskFunction_t         = void (*)(void *);
using configSTACK_DEPTH_TYPE = std::uint32_t;

inline constexpr BaseType_t pdTRUE  = 1;
inline constexpr BaseType_t pdFALSE = 0;

inline constexpr TickType_t portMAX_DELAY = 0xFFFFFFFFu;

// The host clock assumes a 1000 Hz tick so a millisecond maps to one tick — the
// ESP-IDF default rate. The wrap-safe comparison at ~49.7 days is an on-bench
// concern, not exercised by the host compile-proof.
constexpr TickType_t pdMS_TO_TICKS(std::uint32_t ms) noexcept
{
    return ms;
}

namespace plexus::freertos::detail {

// The real kernel queue copies item_size bytes by value; the host stand-in mirrors
// that — it stores whole item-sized records, not a fixed pointer, so a widened item
// (a multi-field POD) round-trips intact off-target instead of truncating to its
// first word.
struct host_queue
{
    std::uint32_t                       item_size;
    std::deque<std::vector<std::byte>>  items;
};

}

using QueueHandle_t = plexus::freertos::detail::host_queue *;

inline TickType_t xTaskGetTickCount() noexcept
{
    using clock                           = std::chrono::steady_clock;
    static const auto               start = clock::now();
    const auto                      since = clock::now() - start;
    const std::chrono::milliseconds ms    = std::chrono::duration_cast<std::chrono::milliseconds>(since);
    return static_cast<TickType_t>(ms.count());
}

inline QueueHandle_t xQueueCreate(std::uint32_t /*length*/, std::uint32_t item_size)
{
    return new plexus::freertos::detail::host_queue{item_size, {}};
}

inline void vQueueDelete(QueueHandle_t q) noexcept
{
    delete q;
}

// Non-blocking host semantics: receive returns pdFALSE when the queue is empty,
// regardless of the requested wait — the host loop never blocks (the device
// block-with-timeout lives in the super-loop driver, not here).
inline BaseType_t xQueueReceive(QueueHandle_t q, void *out, TickType_t /*wait*/)
{
    if(!q || q->items.empty())
        return pdFALSE;
    std::memcpy(out, q->items.front().data(), q->item_size);
    q->items.pop_front();
    return pdTRUE;
}

inline BaseType_t xQueueSend(QueueHandle_t q, const void *item, TickType_t /*wait*/)
{
    if(!q)
        return pdFALSE;
    const auto *bytes = static_cast<const std::byte *>(item);
    q->items.emplace_back(bytes, bytes + q->item_size);
    return pdTRUE;
}

inline BaseType_t xQueueSendFromISR(QueueHandle_t q, const void *item, BaseType_t *woken)
{
    if(woken)
        *woken = pdFALSE;
    return xQueueSend(q, item, 0);
}

inline void vTaskDelay(TickType_t /*ticks*/) noexcept
{
}

namespace plexus::freertos::detail {
// A host spy for the RX-task self-delete: on-target a task that returns aborts, so the trampoline calls
// vTaskDelete(nullptr) at loop exit. Off-target it is a no-op the seam test reads to prove the trampoline
// self-deletes rather than falling off the end.
inline int          host_vtask_delete_calls = 0;
inline TaskHandle_t host_vtask_delete_last  = nullptr;
}

inline void vTaskDelete(TaskHandle_t task) noexcept
{
    ++plexus::freertos::detail::host_vtask_delete_calls;
    plexus::freertos::detail::host_vtask_delete_last = task;
}

// The host suite never schedules a real task; this stand-in lets the run_task spawn
// path compile and link off-target. It reports success without running the trampoline.
inline BaseType_t xTaskCreate(TaskFunction_t /*code*/, const char * /*name*/, configSTACK_DEPTH_TYPE /*stack*/, void * /*params*/, UBaseType_t /*prio*/, TaskHandle_t * /*out*/)
{
    return pdTRUE;
}

inline std::uint32_t ulTaskNotifyTake(BaseType_t /*clear*/, TickType_t /*wait*/) noexcept
{
    return 0;
}

inline void vTaskNotifyGiveFromISR(void * /*task*/, BaseType_t *woken) noexcept
{
    if(woken)
        *woken = pdFALSE;
}

#else

    #include "freertos/FreeRTOS.h"
    #include "freertos/queue.h"
    #include "freertos/task.h"

#endif

#endif
