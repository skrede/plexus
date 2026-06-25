#ifndef HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_POLICY_H
#define HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_POLICY_H

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_stub_channel.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/mcu_byte_owner.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <utility>

namespace plexus::freertos {

// The constrained-target Policy: bundles the cooperative super-loop executor (by
// reference, the hot-path substrate), the no-op stub byte_channel (the substrate is
// transport-free at this stage), and the tick-backed poll-fired timer, plus the
// fixed in-place byte_owner the receive seam binds wire_bytes views to — the one
// member that diverges from every other substrate's heap-and-atomic-refcount owner. The
// static post() forwards onto the executor. The static_assert below is the
// compile-time gate proving a fully non-asio substrate satisfies the seam: a single
// concept diagnostic fires here if any member or the post() signature drifts.
struct freertos_policy
{
    using executor_type     = freertos_executor &;
    using byte_channel_type = freertos_stub_channel;
    using timer_type        = freertos_timer;
    using byte_owner        = mcu_byte_owner;

    // Fully-qualify plexus::detail here: an io/host-shim detail namespace is in scope
    // inside plexus::freertos and would shadow the bare detail:: lookup (the documented
    // move_only_function shadowing pitfall).
    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::freertos::freertos_policy>, "freertos_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
