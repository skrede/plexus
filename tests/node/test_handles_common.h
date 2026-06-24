// The pub/sub handle-semantics oracle, over the deterministic inproc backend. It proves
// the move-only RAII contract and the ctor-canonical registration/retire of the publisher
// and subscriber handles. Two nodes over a shared inproc bus + static_discovery,
// eager-dialing so awareness converges to a live connection on a single drain.
#pragma once

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/node_name.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <type_traits>

namespace handles_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node       = plexus::node<inproc_policy, inproc_transport<>>;
using inproc_publisher  = plexus::publisher<>;
using inproc_subscriber = plexus::subscriber<>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Only the SUBSCRIBER node dials (eager); the publisher node stays lazy and merely
// accepts. With both nodes eager over a shared bus they would mutually dial and form
// TWO sessions per peer (the simultaneous-connect property), double-delivering every
// publish — a single-dialer topology keeps exactly one connection, the realistic shape.
inline plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
    opts.redial_seed  = 0xC0FFEEu;
    opts.dial_eagerly = eager;
    return opts;
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A two-node net: A subscribes, B publishes. Both listen and discover each other over a
// shared static_discovery, eager-dialing to a live connection.
struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    net()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }

    void drive()
    {
        ex.drain();
    }

    std::size_t a_demand_for(const std::string &fqn)
    {
        std::size_t count = 0;
        for(const auto &d : a.router().messages().remembered_topics(plexus::io::node_name_of(id_b)))
            if(d.fqn == fqn)
                ++count;
        return count;
    }
};

} // namespace handles_fixture
