#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_POLICY_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_POLICY_H

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <chrono>
#include <memory>
#include <utility>

namespace plexus::inproc {

// The deterministic inproc Policy: bundles the step-executor (by reference, the
// hot-path substrate), the inproc byte_channel, and the virtual-clock timer, plus
// the shared_ptr<const void> byte_owner the receive seam binds wire_bytes views to.
// The static post() forwards onto the executor. The static_assert below is the
// compile-time gate proving a second, backend-independent substrate satisfies the seam.
struct inproc_policy
{
    using executor_type     = inproc_executor<> &;
    using byte_channel_type = inproc_channel<>;
    using timer_type        = inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::inproc::inproc_policy>, "inproc_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
