#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_EXECUTOR_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_EXECUTOR_H

#include "plexus/inproc/inproc_bus.h"

#include "plexus/inproc/detail/inproc_ingress.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/driver_record.h"

#include <chrono>
#include <thread>
#include <vector>
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
// Work from another thread enters through post_from_thread; a later step appends it behind the
// work already posted.
template<typename Clock = std::chrono::steady_clock>
class inproc_executor
{
public:
    explicit inproc_executor(inproc_bus<Clock> &bus)
            : m_head(0)
            , m_bus(bus)
            , m_ingress()
            , m_timers()
            , m_driver()
            , m_posted()
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

    void post_from_thread(detail::move_only_function<void()> fn)
    {
        m_ingress.push(std::move(fn));
    }

    bool running_in_this_thread() noexcept
    {
        return m_driver.running_in(std::this_thread::get_id());
    }

    bool inside_executor() const noexcept
    {
        return m_driver.inside(std::this_thread::get_id());
    }

    void release_driver() noexcept
    {
        m_driver.release();
    }

    bool step()
    {
        const std::thread::id self = std::this_thread::get_id();
        const detail::driver_record<std::thread::id>::step_scope scope(m_driver, self);
        m_driver.claim_step(self, "step");
        m_ingress.drain_into(m_posted);
        if(m_head < m_posted.size())
            return run_next_posted();

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
    std::size_t m_head;
    inproc_bus<Clock> &m_bus;
    detail::inproc_ingress m_ingress;
    std::vector<inproc_timer<Clock> *> m_timers;
    detail::driver_record<std::thread::id> m_driver;
    std::vector<detail::move_only_function<void()>> m_posted;

    bool run_next_posted()
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
};

}

#endif
