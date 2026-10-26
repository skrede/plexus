#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_FREERTOS_LOOPBACK_POLICY_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_FREERTOS_LOOPBACK_POLICY_H

#include "plexus/io/process_loopback_channel.h"

#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/mcu_byte_owner.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/policy.h"
#include "plexus/executor_verbs.h"

namespace plexus::testing {

// The on-device intra-node shape over the FreeRTOS executor, compiled against the host shim.
struct freertos_loopback_policy : plexus::executor_verbs<plexus::freertos::freertos_executor>
{
    using byte_channel_type = plexus::io::process_loopback_channel<freertos_loopback_policy>;
    using timer_type        = plexus::freertos::freertos_timer;
    using byte_owner        = plexus::freertos::mcu_byte_owner;
};

static_assert(plexus::Policy<freertos_loopback_policy>);

}

#endif
