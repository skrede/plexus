#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_POLICY_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_POLICY_H

#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/mcu_byte_owner.h"

#include "plexus/policy.h"
#include "plexus/executor_verbs.h"

namespace plexus::freertos {

// The constrained-target Policy for the real TCP byte_channel: freertos_policy with the
// transport-free stub swapped for lwip_channel over a connected stream_socket (the same swap
// the uart policy makes). The Socket parameter is the injection seam the transport dials over — the
// on-target lwIP socket on hardware, the host's POSIX socket under test — so a single
// Policy shape serves both without the lib naming the test-side socket.
template<plexus::stream::stream_socket Socket>
struct lwip_policy : plexus::executor_verbs<freertos_executor>
{
    using byte_channel_type = lwip_channel<Socket>;
    using timer_type        = freertos_timer;
    using byte_owner        = mcu_byte_owner;
};

}

static_assert(plexus::Policy<plexus::freertos::lwip_policy<plexus::freertos::detail::null_stream_socket>>,
              "lwip_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
