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
    #include <mutex>
    #include <atomic>
    #include <chrono>
    #include <vector>
    #include <cstdint>
    #include <cstring>
    #include <condition_variable>

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

// The real kernel queue copies item_size bytes by value into a fixed number of slots and
// is safe to use from any task; the host stand-in mirrors all three, so a widened item
// round-trips intact, a full queue refuses a zero-wait send and parks a waiting one, and
// a cross-thread test races nothing.
struct host_queue
{
    std::uint32_t length;
    std::uint32_t item_size;
    std::mutex mutex;
    std::condition_variable room;
    std::deque<std::vector<std::byte>> items;
};

// A host spy: how many senders are parked on a full queue right now. It is raised under the
// queue's lock, so a test that sees it raised knows the sender is past everything it did
// before the send.
inline std::atomic<std::uint32_t> host_parked_senders{0};

// One tick is one millisecond, as xTaskGetTickCount counts; portMAX_DELAY waits indefinitely.
inline bool host_wait_for_room(host_queue &q, std::unique_lock<std::mutex> &lock, TickType_t wait)
{
    const auto has_room = [&q] { return q.items.size() < q.length; };
    if(has_room() || wait == 0)
        return has_room();
    ++host_parked_senders;
    bool got = true;
    if(wait == portMAX_DELAY)
        q.room.wait(lock, has_room);
    else
        got = q.room.wait_for(lock, std::chrono::milliseconds{wait}, has_room);
    --host_parked_senders;
    return got;
}

// Host threads stand in for tasks. A thread_local's address can pass to a later thread once its owner exits, so each
// thread draws an id no other thread ever holds.
inline std::atomic<std::uintptr_t> host_last_task_id{0};
inline thread_local std::uintptr_t host_task_id = 0;

}

using QueueHandle_t = plexus::freertos::detail::host_queue *;

inline TickType_t xTaskGetTickCount() noexcept
{
    using clock                        = std::chrono::steady_clock;
    static const auto start            = clock::now();
    const auto since                   = clock::now() - start;
    const std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(since);
    return static_cast<TickType_t>(ms.count());
}

inline QueueHandle_t xQueueCreate(std::uint32_t length, std::uint32_t item_size)
{
    return new plexus::freertos::detail::host_queue{length, item_size, {}, {}, {}};
}

inline void vQueueDelete(QueueHandle_t q) noexcept
{
    delete q;
}

// Non-blocking host semantics: receive returns pdFALSE when the queue is empty,
// regardless of the requested wait — the host loop never blocks (the device
// block-with-timeout lives in the super-loop driver, not here). A receive wakes a
// sender parked on the full queue.
inline BaseType_t xQueueReceive(QueueHandle_t q, void *out, TickType_t /*wait*/)
{
    if(!q)
        return pdFALSE;
    const std::lock_guard<std::mutex> lock(q->mutex);
    if(q->items.empty())
        return pdFALSE;
    std::memcpy(out, q->items.front().data(), q->item_size);
    q->items.pop_front();
    q->room.notify_one();
    return pdTRUE;
}

// A full queue refuses a zero-wait send at once and parks any other sender until a receive
// makes room or the wait elapses, as the kernel does.
inline BaseType_t xQueueSend(QueueHandle_t q, const void *item, TickType_t wait)
{
    if(!q)
        return pdFALSE;
    std::unique_lock<std::mutex> lock(q->mutex);
    if(!plexus::freertos::detail::host_wait_for_room(*q, lock, wait))
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

inline TaskHandle_t xTaskGetCurrentTaskHandle() noexcept
{
    std::uintptr_t &id = plexus::freertos::detail::host_task_id;
    if(id == 0)
        id = ++plexus::freertos::detail::host_last_task_id;
    return reinterpret_cast<TaskHandle_t>(id);
}

inline void vTaskDelay(TickType_t /*ticks*/) noexcept
{
}

namespace plexus::freertos::detail {
// A host spy for the RX-task self-delete: on-target a task that returns aborts, so the trampoline calls
// vTaskDelete(nullptr) at loop exit. Off-target it is a no-op the seam test reads to prove the trampoline
// self-deletes rather than falling off the end.
inline int host_vtask_delete_calls         = 0;
inline TaskHandle_t host_vtask_delete_last = nullptr;
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
