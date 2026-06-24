#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_POLICY_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_POLICY_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <utility>

namespace plexus::asio {

// The production asio Policy: bundles the io_context executor (carried by
// reference, the hot-path substrate), the TCP byte_channel, and the steady-timer,
// plus the shared_ptr<const void> byte_owner the receive seam binds wire_bytes
// views to. The static post() forwards onto the io_context. The static_assert
// below is the compile-time gate proving the production transport satisfies the
// seam — the third backend-independent data point for the Policy concept.
struct asio_policy
{
    using executor_type     = ::asio::io_context &;
    using byte_channel_type = asio_channel;
    using timer_type        = asio_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::asio::asio_policy>, "asio_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
