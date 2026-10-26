#ifndef HPP_GUARD_PLEXUS_DETAIL_EXECUTOR_AFFINITY_H
#define HPP_GUARD_PLEXUS_DETAIL_EXECUTOR_AFFINITY_H

#include <atomic>
#include <concepts>
#include <type_traits>

namespace plexus::detail {

// Learns which thread drives an executor. The first observation binds its caller; the first observation made inside
// one of the executor's handlers confirms that thread as the driver, handing over from a setup thread once; after that
// every other thread, a second concurrent runner included, is not the driver until release returns the record to
// unbound. Gap: a worker whose call is the very first observation stays the driver until a handler observation hands
// over. A default-constructed Id means unbound.
template<typename Id>
    requires std::is_trivially_copyable_v<Id> && std::equality_comparable<Id>
class executor_affinity
{
public:
    executor_affinity() noexcept
            : m_bound{Id{}}
            , m_confirmed{Id{}}
    {
    }

    bool observe(Id self, bool inside_handler) noexcept
    {
        const Id confirmed = m_confirmed.load(std::memory_order_acquire);
        if(confirmed != Id{})
            return confirmed == self;
        if(inside_handler)
            return claim(m_confirmed, self) == self;
        const Id bound       = claim(m_bound, self);
        const Id handed_over = m_confirmed.load(std::memory_order_acquire);
        return (handed_over != Id{} ? handed_over : bound) == self;
    }

    void release() noexcept
    {
        m_confirmed.store(Id{}, std::memory_order_release);
        m_bound.store(Id{}, std::memory_order_release);
    }

private:
    std::atomic<Id> m_bound;
    std::atomic<Id> m_confirmed;

    static Id claim(std::atomic<Id> &slot, Id self) noexcept
    {
        Id holder{};
        if(slot.compare_exchange_strong(holder, self, std::memory_order_acq_rel, std::memory_order_acquire))
            return self;
        return holder;
    }
};

}

#endif
