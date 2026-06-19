#ifndef HPP_GUARD_TESTS_INTEGRATION_SPINE_LIFECYCLE_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SPINE_LIFECYCLE_INPROC_COMMON_H

// The declaration-lifecycle timeline oracle: a custom observer reconstructs the ordered
// per-node session timeline over a single-node inproc composition — the node's own
// create/destroy, a publisher's declare and its handle drop, a subscriber's register and
// retire. Every edge is POSTED on the executor, so the test witnesses an edge only after
// it drives the executor (the posted-delivery DoS-guard contract).
//
// The destroy + publisher-drop edges are the load-bearing case: the node and the publisher
// handle are destroyed in an INNER scope while the FIXTURE-owned executor stays live in the
// outer scope, then the still-live executor is pumped to surface the posted edges. The
// executor is the fixture's, NOT node-owned — pumping a node-owned executor after the node
// returned would be use-after-free. A separate cell proves a moved-from handle (the null
// seam-ctx sentinel) fires no drop edge.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/message_info.h"
#include "plexus/io/observation_events.h"

#include "recording_observer.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace spine_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0x57A11Du;
    opts.dial_eagerly = false;
    return opts;
}

inline plexus::node_options make_eager_opts()
{
    plexus::node_options opts = make_opts();
    opts.dial_eagerly         = true;
    return opts;
}

// The fixture OWNS the executor + bus + transport + discovery; a node is constructed in an
// inner scope over this borrowed substrate, so the executor outlives the node and can be
// pumped after the node's dtor returns.
struct fixture
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    static_discovery   disc{{}};

    void drive() { ex.drain(); }
};

// A two-node fixture for the per-peer wire-unsubscribe edge: the producer node owns the
// observer; when the subscriber node's last local subscriber retires, it sends a wire
// unsubscribe that drives the producer's detach -> on_qos_change{unsubscribed}.
struct pair_fixture
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB1)};

    plexus::node_options opts_a{make_eager_opts()};
    plexus::node_options opts_b{make_eager_opts()};

    inproc_node a{ex, disc, id_a, ta, opts_a};
    inproc_node b{ex, disc, id_b, tb, opts_b};

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

#endif
