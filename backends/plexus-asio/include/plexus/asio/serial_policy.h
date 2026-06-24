#ifndef HPP_GUARD_PLEXUS_ASIO_SERIAL_POLICY_H
#define HPP_GUARD_PLEXUS_ASIO_SERIAL_POLICY_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/serial_channel.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <utility>

namespace plexus::asio {

struct serial_policy
{
    using executor_type     = ::asio::io_context &;
    using byte_channel_type = serial_channel;
    using timer_type        = asio_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::asio::serial_policy>, "serial_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
