#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_EXECUTOR_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_EXECUTOR_H

#include "plexus/inproc/inproc_bus.h"

#include "plexus/detail/compat.h"

#include <vector>
#include <chrono>
#include <cstddef>
#include <utility>
#include <algorithm>

namespace plexus::inproc {

template<typename Clock>
class inproc_timer;

// Cooperative single-thread step-executor over an inproc_bus. step() advances one unit of work at a
// fixed priority — posted callbacks, then a single bus delivery, then expired timers — and returns
// false only at quiescence; drain() steps to quiescence. Timers are checked only once the ready
// work is exhausted (the asio reactor discipline), so the steady delivery loop pays no clock read
// per step. Routing every byte delivery through step() is what makes inproc delivery posted-only.
template<typename Clock = std::chrono::steady_clock>
class inproc_executor
{
public:
    explicit inproc_executor(inproc_bus<Clock> &bus)
            : m_bus(bus)
    {
    }

    inproc_executor(const inproc_executor &)            = delete;
    inproc_executor &operator=(const inproc_executor &) = delete;
    inproc_executor(inproc_executor &&)                 = delete;
    inproc_executor &operator=(inproc_executor &&)      = delete;

    void post(detail::move_only_function<void()> fn)
    {
        m_posted.push_back(std::move(fn));
    }

    bool step()
    {
        if(m_head < m_posted.size())
        {
            auto fn = std::move(m_posted[m_head]);
            // Recycle the buffer the moment it drains empty rather than pop_front-ing per step: the
            // vector keeps its capacity across the clear, so a steady post-drain loop reallocates
            // nothing (a std::deque recenters its block map periodically, which a zero-alloc gate on
            // any posted path would otherwise trip).
            if(++m_head == m_posted.size())
            {
                m_posted.clear();
                m_head = 0;
            }
            fn();
            return true;
        }

        if(m_bus.deliver_one())
            return true;

        return fire_due_timer();
    }

    void drain()
    {
        while(step())
        {
        }
    }

    void register_timer(inproc_timer<Clock> *t)
    {
        if(std::find(m_timers.begin(), m_timers.end(), t) == m_timers.end())
            m_timers.push_back(t);
    }

    void deregister_timer(inproc_timer<Clock> *t) noexcept
    {
        std::erase(m_timers, t);
    }

    inproc_bus<Clock> &bus() noexcept
    {
        return m_bus;
    }

private:
    // The clock is read only here, and only when some timer could actually fire.
    bool fire_due_timer()
    {
        const bool any_armed = std::any_of(m_timers.begin(), m_timers.end(), [](const inproc_timer<Clock> *t) { return t->armed(); });
        if(!any_armed)
            return false;
        const auto now = Clock::now();
        for(auto *t : m_timers)
            if(t->try_fire(now))
                return true;
        return false;
    }

    inproc_bus<Clock> &m_bus;
    std::vector<detail::move_only_function<void()>> m_posted;
    std::size_t m_head{0};
    std::vector<inproc_timer<Clock> *> m_timers;
};

}

#endif
