#ifndef HPP_GUARD_PLEXUS_TESTING_PUMP_H
#define HPP_GUARD_PLEXUS_TESTING_PUMP_H

#include <chrono>
#include <cstdint>

namespace plexus::testing {

inline constexpr std::chrono::milliseconds k_default_pump_bound{6000};

// A stopped asio io_context runs nothing until restart(), so an executor that has one is restarted before every poll.
template<typename Io, typename Pred>
bool pump_until(Io &io, Pred pred, std::chrono::milliseconds bound = k_default_pump_bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
    {
        if constexpr(requires { io.restart(); })
            io.restart();
        io.poll();
    }
    return pred();
}

template<typename Executor, typename Pred>
bool pump_steps_until(Executor &ex, Pred done, std::int32_t step_budget)
{
    for(std::int32_t step = 0; step < step_budget && !done(); ++step)
    {
        if(!ex.step())
            break;
    }
    return done();
}

}

#endif
