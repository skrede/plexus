#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_FREERTOS_INGRESS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_FREERTOS_INGRESS_H

#include "plexus/freertos/posted_work.h"

#include "plexus/detail/compat.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <utility>

namespace plexus::freertos::detail {

// Adapts a callable into the caller-owned posted_work the kernel queue carries, through a fixed pool of Depth slots. A
// post that finds no free slot, or whose send fails, returns false and is counted in dropped(): never an allocation,
// never a block.
template<std::uint32_t Depth>
class freertos_ingress
{
    struct slot
    {
        std::atomic<bool> busy{false};
        plexus::detail::move_only_function<void()> fn;
    };

public:
    freertos_ingress()
            : m_dropped{0}
            , m_slots()
    {
    }

    freertos_ingress(const freertos_ingress &)            = delete;
    freertos_ingress &operator=(const freertos_ingress &) = delete;

    template<typename Send>
    bool post(plexus::detail::move_only_function<void()> fn, Send send)
    {
        slot *const claimed = claim();
        if(claimed == nullptr)
        {
            count_drop();
            return false;
        }
        claimed->fn = std::move(fn);
        if(send(posted_work{&run, claimed}))
            return true;
        release(*claimed);
        count_drop();
        return false;
    }

    std::uint32_t dropped() const noexcept
    {
        return m_dropped.load(std::memory_order_relaxed);
    }

private:
    std::atomic<std::uint32_t> m_dropped;
    std::array<slot, Depth> m_slots;

    slot *claim() noexcept
    {
        for(slot &candidate : m_slots)
        {
            bool expected = false;
            if(candidate.busy.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed))
                return &candidate;
        }
        return nullptr;
    }

    void count_drop() noexcept
    {
        m_dropped.fetch_add(1, std::memory_order_relaxed);
    }

    static void release(slot &claimed) noexcept
    {
        claimed.fn = nullptr;
        claimed.busy.store(false, std::memory_order_release);
    }

    static void run(void *ctx) noexcept
    {
        slot &claimed = *static_cast<slot *>(ctx);
        claimed.fn();
        release(claimed);
    }
};

}

#endif
