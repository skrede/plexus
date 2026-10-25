#ifndef HPP_GUARD_PLEXUS_TLS_TLS_POLICY_H
#define HPP_GUARD_PLEXUS_TLS_TLS_POLICY_H

#include "plexus/tls/tls_channel.h"

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/io_context_verbs.h"

#include "plexus/policy.h"

#include <memory>

namespace plexus::tls {

// The TLS Policy: the asio Policy with the byte_channel swapped to the ssl::stream-backed tls_channel.
struct tls_policy : plexus::asio::io_context_verbs
{
    using byte_channel_type = tls_channel;
    using timer_type        = plexus::asio::asio_timer;
    using byte_owner        = std::shared_ptr<const void>;
};

}

static_assert(plexus::Policy<plexus::tls::tls_policy>, "tls_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
