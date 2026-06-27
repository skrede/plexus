#ifndef HPP_GUARD_PLEXUS_EXAMPLES_MCU_ONBOARD_POLICY_H
#define HPP_GUARD_PLEXUS_EXAMPLES_MCU_ONBOARD_POLICY_H

#include "plexus/io/process_loopback_channel.h"

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/mcu_byte_owner.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <utility>

namespace example {

// The onboard intra-node Policy: it reuses the constrained-target executor, timer, and in-place
// byte_owner of the FreeRTOS substrate, but binds the intra-node self-channel as its
// byte_channel_type instead of a wire transport. A single-transport node composed over this
// policy delivers its own publications to its own subscribers on-device with zero link — the
// typed fast path engages with zero erasure. The static post() forwards onto the executor; the
// static_assert proves this fully transport-free substrate satisfies the seam.
struct onboard_policy
{
    using executor_type     = plexus::freertos::freertos_executor &;
    using byte_channel_type = plexus::io::process_loopback_channel<onboard_policy>;
    using timer_type        = plexus::freertos::freertos_timer;
    using byte_owner        = plexus::freertos::mcu_byte_owner;

    // Fully-qualify plexus::detail: an io/host-shim detail namespace is in scope here and would
    // shadow the bare detail:: lookup (the documented move_only_function shadowing pitfall).
    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

static_assert(plexus::Policy<example::onboard_policy>, "onboard_policy must satisfy Policy — check the loopback channel/timer constructors and post()");

#endif
