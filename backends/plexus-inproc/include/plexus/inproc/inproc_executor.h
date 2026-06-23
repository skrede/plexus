#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_EXECUTOR_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_EXECUTOR_H

#include "plexus/inproc/inproc_bus.h"

#include "plexus/detail/compat.h"

#include <deque>
#include <vector>
#include <chrono>
#include <utility>
#include <algorithm>

namespace plexus::inproc {

template<typename Clock>
class inproc_timer;

// Cooperative single-thread step-executor over an inproc_bus. step() advances
// the system by one unit of work with a fixed priority — posted callbacks, then
// a single bus delivery, then expired timers — and returns false only at
// quiescence; drain() steps to quiescence. Timers are checked only once the
// ready work (posted + bus) is exhausted, the asio reactor discipline: a due
// timer fires within the same drain pass, and the steady delivery loop pays no
// clock read per step (the read was 20%+ of the in-process publish cycle). The
// virtual clock makes timer firing deterministic, and routing every byte
// delivery through step() is what makes inproc delivery posted-only rather
// than synchronous.
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

    void post(detail::move_only_function<void()> fn) { m_posted.push_back(std::move(fn)); }

    bool step()
    {
        if(!m_posted.empty())
        {
            auto fn = std::move(m_posted.front());
            m_posted.pop_front();
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

    void deregister_timer(inproc_timer<Clock> *t) noexcept { std::erase(m_timers, t); }

    inproc_bus<Clock> &bus() noexcept { return m_bus; }

private:
    // The clock is read only here, and only when some timer could actually fire —
    // never for an armed-but-handlerless or cancelled timer.
    bool fire_due_timer()
    {
        const bool any_armed = std::any_of(m_timers.begin(), m_timers.end(),
                                           [](const inproc_timer<Clock> *t) { return t->armed(); });
        if(!any_armed)
            return false;
        const auto now = Clock::now();
        for(auto *t : m_timers)
            if(t->try_fire(now))
                return true;
        return false;
    }

    inproc_bus<Clock>                             &m_bus;
    std::deque<detail::move_only_function<void()>> m_posted;
    std::vector<inproc_timer<Clock> *>             m_timers;
};

}

#endif
