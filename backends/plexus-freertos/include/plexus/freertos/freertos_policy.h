#ifndef HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_POLICY_H
#define HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_POLICY_H

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_stub_channel.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/mcu_byte_owner.h"

#include "plexus/policy.h"
#include "plexus/executor_verbs.h"

namespace plexus::freertos {

// The transport-free constrained-target Policy. Its fixed in-place byte_owner, which the receive seam binds wire_bytes
// views to, is the one member that diverges from every other substrate's heap-and-atomic-refcount owner.
struct freertos_policy : plexus::executor_verbs<freertos_executor>
{
    using byte_channel_type = freertos_stub_channel;
    using timer_type        = freertos_timer;
    using byte_owner        = mcu_byte_owner;
};

}

static_assert(plexus::Policy<plexus::freertos::freertos_policy>, "freertos_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
