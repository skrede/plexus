#ifndef HPP_GUARD_PLEXUS_ASIO_NODE_ALIASES_H
#define HPP_GUARD_PLEXUS_ASIO_NODE_ALIASES_H

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/detail/same_host_shm_config.h"

#include "plexus/node.h"

namespace plexus::asio {

// The common single-wire asio composition.
using node = basic_node<asio_policy, asio_transport>;

#if PLEXUS_SAME_HOST_SHM
namespace shm {

// The shm member leads the pack: the same-host preference hook scans for the first
// local_fast_eligible candidate, so the shm leaf must precede the unix and tcp fallbacks.
using node = basic_node<asio_policy, shm_member, unix_transport, asio_transport>;

}
#endif

}

#endif
