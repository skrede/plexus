#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_RX_LIFECYCLE_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_RX_LIFECYCLE_H

#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/lwip_rx_slot_pool.h"
#include "plexus/freertos/detail/freertos_host_shim.h"

#include <cstddef>

namespace plexus::freertos::detail {

template<typename Owner, std::size_t Slots, std::size_t Capacity>
class lwip_rx_lifecycle
{
public:
    using slot_type = rx_slot<Owner, Capacity>;

    // The RX hand-off pool holds k_slots buffers and must stay within the executor queue depth, so
    // it never holds more in-flight slots than the queue can carry a post for.
    static constexpr std::size_t k_slots = Slots;
    static_assert(k_slots <= freertos_executor::k_queue_depth, "the RX pool must not outsize the executor queue depth");

    explicit lwip_rx_lifecycle(Owner &owner)
            : m_pool(owner)
    {
    }

    lwip_rx_lifecycle(const lwip_rx_lifecycle &)            = delete;
    lwip_rx_lifecycle &operator=(const lwip_rx_lifecycle &) = delete;

    slot_type *acquire(TickType_t wait) noexcept
    {
        return m_pool.acquire(wait);
    }

    void release(slot_type &slot) noexcept
    {
        m_pool.release(slot);
    }

    static void invoke_feed(void *ctx) noexcept
    {
        auto &slot = *static_cast<slot_type *>(ctx);
        slot.owner->feed_from_slot(slot);
    }

private:
    lwip_rx_slot_pool<Owner, Slots, Capacity> m_pool;
};

}

#endif
