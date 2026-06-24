#ifndef HPP_GUARD_PLEXUS_IO_PEER_CONTEXT_H
#define HPP_GUARD_PLEXUS_IO_PEER_CONTEXT_H

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/epoch_source.h"
#include "plexus/io/host_fingerprint.h"

#include <memory>
#include <string>

namespace plexus::io {

// This record OUTLIVES every peer_session built from it, so a redial draws a STRICTLY
// later epoch from the same source — the cross-incarnation monotonicity the staleness
// gate relies on is structural. has_ever_connected and same_host live here, not on the
// session, because a session-local flag would reset on every reconnect rebuild.
template<typename Policy>
    requires plexus::Policy<Policy>
struct peer_context
{
    using channel_type = typename Policy::byte_channel_type;

    std::unique_ptr<channel_type> channel;
    node_id peer_id{};
    std::string node_name;
    endpoint dial_endpoint;
    epoch_source epochs;
    bool has_ever_connected{false};
    bool same_host{false}; // fail-closed: not co-located until proven
};

}

#endif
