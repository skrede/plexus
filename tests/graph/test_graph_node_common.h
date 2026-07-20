#ifndef HPP_GUARD_PLEXUS_TESTS_GRAPH_NODE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_GRAPH_NODE_COMMON_H

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <string>
#include <cstdint>
#include <string_view>

namespace graph_node_fixture {

using inproc_node = plexus::node<plexus::inproc::inproc_policy, plexus::inproc::inproc_transport<>>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::io::endpoint make_ep(std::string_view address)
{
    return plexus::io::endpoint{"inproc", std::string{address}};
}

inline plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect   = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

struct host
{
    plexus::inproc::inproc_bus<>      bus;
    plexus::inproc::inproc_executor<> ex{bus};
    plexus::inproc::inproc_transport<> transport{ex, bus};
    plexus::discovery::static_discovery disc{{}};
};

}

#endif
