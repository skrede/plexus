#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_POLICY_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_POLICY_H

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/mcu_byte_owner.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <utility>

namespace plexus::freertos {

// The constrained-target Policy for the real TCP byte_channel: freertos_policy with the
// transport-free stub swapped for lwip_channel over a connected stream_socket (the same swap
// v0.2.5 did stub->uart). The Socket parameter is the injection seam the transport dials over — the
// on-target lwIP socket on hardware, the host's POSIX socket in the loopback slice — so a single
// Policy shape serves both without the lib naming the test-side socket.
//
// Fully-qualify plexus::detail here: an io/host-shim detail namespace is in scope inside
// plexus::freertos and would shadow the bare detail:: lookup (the documented move_only_function
// shadowing pitfall).
template<plexus::stream::stream_socket Socket>
struct lwip_policy
{
    using executor_type     = freertos_executor &;
    using byte_channel_type = lwip_channel<Socket>;
    using timer_type        = freertos_timer;
    using byte_owner        = mcu_byte_owner;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::freertos::lwip_policy<plexus::freertos::detail::null_stream_socket>>,
              "lwip_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
