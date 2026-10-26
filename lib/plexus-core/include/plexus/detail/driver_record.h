#ifndef HPP_GUARD_PLEXUS_DETAIL_DRIVER_RECORD_H
#define HPP_GUARD_PLEXUS_DETAIL_DRIVER_RECORD_H

#include "plexus/detail/executor_affinity.h"

#include <atomic>

namespace plexus::detail {

// The driver record of an executor its caller steps by hand: which thread or task is inside a step right now, and the
// affinity state machine fed from that fact. A step nested inside a handler restores the outer step's record on exit.
template<typename Id>
class driver_record
{
public:
    class step_scope
    {
    public:
        step_scope(driver_record &record, Id self) noexcept
                : m_record(record)
                , m_outer(record.m_inside.load(std::memory_order_relaxed))
        {
            m_record.m_inside.store(self, std::memory_order_relaxed);
        }

        step_scope(const step_scope &)            = delete;
        step_scope &operator=(const step_scope &) = delete;

        ~step_scope()
        {
            m_record.m_inside.store(m_outer, std::memory_order_relaxed);
        }

    private:
        driver_record &m_record;
        Id m_outer;
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
