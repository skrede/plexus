#ifndef HPP_GUARD_PLEXUS_MCU_DETAIL_FREERTOS_HOST_SHIM_H
#define HPP_GUARD_PLEXUS_MCU_DETAIL_FREERTOS_HOST_SHIM_H

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
    #include <chrono>
    #include <cstdint>

using TickType_t = std::uint32_t;
using BaseType_t = int;

inline constexpr BaseType_t pdTRUE  = 1;
inline constexpr BaseType_t pdFALSE = 0;

// The host clock assumes a 1000 Hz tick so a millisecond maps to one tick — the
// ESP-IDF default rate. The wrap-safe comparison at ~49.7 days is an on-bench
// concern, not exercised by the host compile-proof.
constexpr TickType_t pdMS_TO_TICKS(std::uint32_t ms) noexcept
{
    return ms;
}

namespace plexus::mcu::detail {

struct host_queue
{
    std::deque<void *> items;
};

}

using QueueHandle_t = plexus::mcu::detail::host_queue *;

inline TickType_t xTaskGetTickCount() noexcept
{
    using clock                           = std::chrono::steady_clock;
    static const auto               start = clock::now();
    const auto                      since = clock::now() - start;
    const std::chrono::milliseconds ms    = std::chrono::duration_cast<std::chrono::milliseconds>(since);
    return static_cast<TickType_t>(ms.count());
}

inline QueueHandle_t xQueueCreate(std::uint32_t /*length*/, std::uint32_t /*item_size*/)
{
    return new plexus::mcu::detail::host_queue;
}

// Non-blocking host semantics: receive returns pdFALSE when the queue is empty,
// regardless of the requested wait — the host loop never blocks (the device
// block-with-timeout lives in the super-loop driver, not here).
inline BaseType_t xQueueReceive(QueueHandle_t q, void *out, TickType_t /*wait*/)
{
    if(!q || q->items.empty())
        return pdFALSE;
    *static_cast<void **>(out) = q->items.front();
    q->items.pop_front();
    return pdTRUE;
}

inline BaseType_t xQueueSend(QueueHandle_t q, const void *item, TickType_t /*wait*/)
{
    if(!q)
        return pdFALSE;
    q->items.push_back(*static_cast<void *const *>(item));
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
