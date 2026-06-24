#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_POLICY_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_POLICY_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/unix_channel.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <utility>

namespace plexus::asio {

// The AF_UNIX asio Policy: a near-verbatim clone of asio_policy swapping the
// byte_channel to the local-stream unix_channel — the io_context executor (carried
// by reference, the hot-path substrate), the steady-timer, and the
// shared_ptr<const void> byte_owner are reused unchanged. The static post()
// forwards onto the io_context. The static_assert below is the compile-time gate
// proving the local-stream transport satisfies the same Policy seam as the TCP
// transport — a second backend-within-asio data point for the concept.
struct unix_policy
{
    using executor_type     = ::asio::io_context &;
    using byte_channel_type = unix_channel;
    using timer_type        = asio_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::asio::unix_policy>, "unix_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
