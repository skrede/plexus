#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_POLICY_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_POLICY_H

#include "plexus/tls/dtls_channel.h"

#include "plexus/asio/asio_timer.h"
#include "plexus/asio/io_context_verbs.h"

#include "plexus/policy.h"

#include <memory>

namespace plexus::tls {

// The DTLS Policy: the TLS Policy over the datagram-backed dtls_channel, which also drives its retransmit off the
// asio timer.
struct dtls_policy : plexus::asio::io_context_verbs
{
    using byte_channel_type = dtls_channel;
    using timer_type        = plexus::asio::asio_timer;
    using byte_owner        = std::shared_ptr<const void>;
};

}

static_assert(plexus::Policy<plexus::tls::dtls_policy>, "dtls_policy must satisfy Policy — check the channel/timer constructors and post()");

#endif
