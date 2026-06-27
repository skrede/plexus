#ifndef HPP_GUARD_PLEXUS_LOOPBACK_NODE_H
#define HPP_GUARD_PLEXUS_LOOPBACK_NODE_H

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/io/intra_node_transport.h"

#include "plexus/discovery/discovery.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

namespace plexus {

// A node whose sole transport is the intra-node self-route: single transport binds the concrete
// process_loopback_channel as the channel_type, so same-node pub/sub delivers zero-copy on the
// typed lane with zero erasure (the default-floor MCU on-board case).
template<typename Policy>
using loopback_node = basic_node<Policy, io::intra_node_transport<Policy>>;

// The default-floor construction layer: it OWNS the intra-node leaf the borrow-model node needs, so
// a caller stands up a self-delivering node with NO transport of their own. The leaf is declared
// before the node so it outlives the borrowed reference the node holds.
template<typename Policy>
class loopback_host
{
public:
    loopback_host(typename Policy::executor_type executor, discovery::discovery &disc, const node_id &id, const node_options &opts = {})
            : m_leaf()
            , m_node(executor, disc, id, m_leaf, opts)
    {
    }

    loopback_host(const loopback_host &)            = delete;
    loopback_host &operator=(const loopback_host &) = delete;
    loopback_host(loopback_host &&)                 = delete;
    loopback_host &operator=(loopback_host &&)      = delete;

    loopback_node<Policy> &node() noexcept
    {
        return m_node;
    }

private:
    io::intra_node_transport<Policy> m_leaf;
    loopback_node<Policy> m_node;
};

}

#endif
