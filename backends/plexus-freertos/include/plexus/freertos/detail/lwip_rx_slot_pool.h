#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_RX_SLOT_POOL_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_RX_SLOT_POOL_H

#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include <array>
#include <cstdint>
#include <cstddef>

namespace plexus::freertos::detail {

// One RX hand-off slot: a fixed-capacity recv buffer plus the byte count and the typed back-pointer
// the executor-side invoker reaches the channel through. POD, pool-resident for the channel's
// lifetime — acquiring it is never an allocation.
template<typename Owner, std::size_t Capacity>
struct rx_slot
{
    std::array<std::byte, Capacity> buffer;
    std::size_t                     len;
    Owner                          *owner;
    std::uint32_t                   index;
};

// A fixed pre-allocated pool of RX hand-off slots whose free-list is a FreeRTOS queue of slot
// indices. The free-list is touched from TWO contexts — acquire() on the RX task, release() on the
// executor task — so it MUST be thread-safe by construction; the index queue is (FreeRTOS queues
// are), where a plain bitset/index-stack would be a data race the host shim cannot surface. The
// queue is pre-filled with every index at construction; acquire blocks-and-bounds when empty.
template<typename Owner, std::size_t Slots, std::size_t Capacity>
class lwip_rx_slot_pool
{
public:
    explicit lwip_rx_slot_pool(Owner &owner)
            : m_slots{}
            , m_free(xQueueCreate(Slots, sizeof(std::uint32_t)))
    {
        for(std::uint32_t i = 0; i < Slots; ++i)
        {
            m_slots[i].owner = &owner;
            m_slots[i].index = i;
            xQueueSend(m_free, &i, 0);
        }
    }

    lwip_rx_slot_pool(const lwip_rx_slot_pool &)            = delete;
    lwip_rx_slot_pool &operator=(const lwip_rx_slot_pool &) = delete;

    ~lwip_rx_slot_pool()
    {
        vQueueDelete(m_free);
    }

    // RX-task side: take a free index off the queue and hand back the slot, or nullptr when the
    // pool is exhausted (the bounded congestion signal — never a heap grow). On-target a non-zero
    // wait parks the RX task until a slot frees instead of dropping.
    rx_slot<Owner, Capacity> *acquire(TickType_t wait) noexcept
    {
        std::uint32_t idx = 0;
        if(xQueueReceive(m_free, &idx, wait) != pdTRUE)
            return nullptr;
        return &m_slots[idx];
    }

    // Executor-task side: return the slot's index to the free queue.
    void release(rx_slot<Owner, Capacity> &slot) noexcept
    {
        xQueueSend(m_free, &slot.index, 0);
    }

private:
    std::array<rx_slot<Owner, Capacity>, Slots> m_slots;
    QueueHandle_t                               m_free;
};

}

#endif
