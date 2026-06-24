#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_POLICY_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_POLICY_H

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/udp_channel.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <utility>

namespace plexus::asio {

// The UDP asio Policy: a near-verbatim clone of unix_policy swapping the
// byte_channel to the connectionless udp_channel — the io_context executor (carried
// by reference, the hot-path substrate), the steady-timer, and the
// shared_ptr<const void> byte_owner are reused unchanged. The static post() forwards
// onto the io_context. The static_assert below is the compile-time gate proving the
// connectionless datagram transport satisfies the same Policy seam as every stream
// transport — a third backend-within-asio data point, and the load-bearing proof the
// NON-stream channel needs no Policy reshape.
struct udp_policy
{
    using executor_type     = ::asio::io_context &;
    using byte_channel_type = udp_channel;
    using timer_type        = asio_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::asio::udp_policy>, "udp_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
