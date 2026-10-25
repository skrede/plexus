#ifndef HPP_GUARD_PLEXUS_ASIO_SERIAL_POLICY_H
#define HPP_GUARD_PLEXUS_ASIO_SERIAL_POLICY_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/serial_channel.h"
#include "plexus/asio/io_context_verbs.h"

#include "plexus/policy.h"

#include <memory>

namespace plexus::asio {

struct serial_policy : io_context_verbs
{
    using byte_channel_type = serial_channel;
    using timer_type        = asio_timer;
    using byte_owner        = std::shared_ptr<const void>;
};

}

static_assert(plexus::Policy<plexus::asio::serial_policy>, "serial_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
