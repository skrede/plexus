#ifndef HPP_GUARD_PLEXUS_TLS_TLS_POLICY_H
#define HPP_GUARD_PLEXUS_TLS_TLS_POLICY_H

#include "plexus/tls/tls_channel.h"

#include "plexus/asio/asio_timer.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <utility>

namespace plexus::tls {

// The TLS Policy: a near-verbatim clone of the asio Policy swapping the
// byte_channel to the ssl::stream-backed tls_channel — the io_context executor
// (carried by reference, the hot-path substrate), the asio steady-timer (REUSED
// from plexus::asio), and the shared_ptr<const void> byte_owner are reused
// unchanged. The static post() forwards onto the io_context. The static_assert
// below proves the crypto transport satisfies the same Policy seam as the
// plaintext transports.
struct tls_policy
{
    using executor_type     = ::asio::io_context &;
    using byte_channel_type = tls_channel;
    using timer_type        = plexus::asio::asio_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::tls::tls_policy>, "tls_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
