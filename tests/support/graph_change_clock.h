#ifndef HPP_GUARD_TESTS_SUPPORT_GRAPH_CHANGE_CLOCK_H
#define HPP_GUARD_TESTS_SUPPORT_GRAPH_CHANGE_CLOCK_H

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/policy.h"
#include "plexus/executor_verbs.h"

#include <chrono>
#include <memory>

namespace plexus::testing::graph_change_fixture {

struct manual_clock
{
    using duration                                   = std::chrono::nanoseconds;
    using rep                                        = duration::rep;
    using period                                     = duration::period;
    using time_point                                 = std::chrono::time_point<manual_clock>;
    [[maybe_unused]] static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept
    {
        return current;
    }
    static void reset() noexcept
    {
        current = time_point{};
    }
    static void advance(duration d) noexcept
    {
        current += d;
    }
};

struct manual_policy : plexus::executor_verbs<plexus::inproc::inproc_executor<manual_clock>>
{
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;
};

static_assert(plexus::Policy<manual_policy>);

}

#endif
