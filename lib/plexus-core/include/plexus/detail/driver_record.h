#ifndef HPP_GUARD_PLEXUS_DETAIL_DRIVER_RECORD_H
#define HPP_GUARD_PLEXUS_DETAIL_DRIVER_RECORD_H

#include "plexus/detail/fail_closed.h"
#include "plexus/detail/executor_affinity.h"

#include <atomic>

namespace plexus::detail {

// The driver record of an executor its caller steps by hand: which thread or task is inside a step right now, and the
// affinity state machine fed from that fact. A step nested inside a handler leaves the record to the outermost step.
template<typename Id>
class driver_record
{
public:
    // A scope remembers only whether its own thread was already inside, so overlapping steppers leave the record empty
    // whichever leaves first.
    class step_scope
    {
    public:
        step_scope(driver_record &record, Id self) noexcept
                : m_self(self)
                , m_nested(record.m_inside.load(std::memory_order_relaxed) == self)
                , m_record(record)
        {
            m_record.m_inside.store(self, std::memory_order_relaxed);
        }

        step_scope(const step_scope &)            = delete;
        step_scope &operator=(const step_scope &) = delete;

        ~step_scope()
        {
            Id own = m_self;
            if(!m_nested)
                m_record.m_inside.compare_exchange_strong(own, Id{}, std::memory_order_relaxed);
        }

    private:
        Id m_self;
        bool m_nested;
        driver_record &m_record;
    };

    driver_record() noexcept
            : m_inside{Id{}}
            , m_affinity()
    {
    }

    bool inside(Id self) const noexcept
    {
        return m_inside.load(std::memory_order_relaxed) == self;
    }

    bool running_in(Id self) noexcept
    {
        return m_affinity.observe(self, inside(self));
    }

#if defined(PLEXUS_AFFINITY_CHECKS)
    void claim_step(Id self, const char *verb) noexcept
    {
        if(!m_affinity.observe(self, true))
            affinity_trip(verb);
    }
#else
    void claim_step(Id, const char *) noexcept
    {
    }
#endif

    void release() noexcept
    {
        m_affinity.release();
    }

private:
    std::atomic<Id> m_inside;
    executor_affinity<Id> m_affinity;
};

}

#endif
