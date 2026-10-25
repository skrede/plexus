#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_POLICY_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_POLICY_H

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/policy.h"
#include "plexus/executor_verbs.h"

#include <chrono>
#include <memory>

namespace plexus::inproc {

// The deterministic inproc Policy; the receive seam binds wire_bytes views to its byte_owner.
struct inproc_policy : plexus::executor_verbs<inproc_executor<>>
{
    using byte_channel_type = inproc_channel<>;
    using timer_type        = inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;
};

}

static_assert(plexus::Policy<plexus::inproc::inproc_policy>, "inproc_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
