#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_POLICY_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_POLICY_H

#include "plexus/tls/dtls_channel.h"

#include "plexus/asio/asio_timer.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <memory>
#include <utility>

namespace plexus::tls {

// The DTLS Policy: the tls_policy bundle with the byte_channel swapped to the
// datagram-backed dtls_channel — the io_context executor (carried by reference,
// the hot-path substrate), the asio steady-timer (REUSED from plexus::asio; DTLS
// also drives its retransmit off it), and the shared_ptr<const void> byte_owner
// are reused unchanged. The static post() forwards onto the io_context. The
// static_assert below proves the secure-datagram transport satisfies the same
// Policy seam as the plaintext and TLS transports.
struct dtls_policy
{
    using executor_type     = ::asio::io_context &;
    using byte_channel_type = dtls_channel;
    using timer_type        = plexus::asio::asio_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::tls::dtls_policy>,
              "dtls_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
