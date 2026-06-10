// The node construction surface oracle: the injected-substrate compile-error proofs
// (omitting executor / discovery / transports / options each makes the node
// non-constructible), the escape hatches returning the live engine objects, and the
// API-06 identity surface (verbatim node_id primary; the deterministic opt-in
// name-hash overload). All over the deterministic inproc policy/transport with a
// static_discovery — no real networking, no owned io_context.

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <type_traits>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000),
                                        std::nullopt, std::nullopt};
}

plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.reconnect = forever_cfg();
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

// One node over the inproc backend with its own borrowed substrate. Member ORDER:
// the bus/executor/transport/discovery BEFORE the node, so the node unwinds before
// the substrate it borrows.
struct host
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};
    plexus::discovery::static_discovery disc{{}};
};

}

// The four injected-substrate omission proofs: the node is NOT constructible when any
// required substrate element is missing (D-06 — there is no owning convenience
// overload, so omission is a compile error, asserted here as non-constructibility).
static_assert(std::is_constructible_v<
                  inproc_node, inproc_executor<> &, plexus::discovery::static_discovery &,
                  const plexus::node_id &, inproc_transport<> &, const plexus::node_options &>,
              "the node IS constructible with the full injected substrate");

static_assert(!std::is_constructible_v<inproc_node>,
              "omitting the entire substrate is a compile error");
static_assert(!std::is_constructible_v<
                  inproc_node, plexus::discovery::static_discovery &, const plexus::node_id &,
                  inproc_transport<> &, const plexus::node_options &>,
              "omitting the executor is a compile error");
static_assert(!std::is_constructible_v<
                  inproc_node, inproc_executor<> &, const plexus::node_id &,
                  inproc_transport<> &, const plexus::node_options &>,
              "omitting the discovery is a compile error");
static_assert(!std::is_constructible_v<
                  inproc_node, inproc_executor<> &, plexus::discovery::static_discovery &,
                  const plexus::node_id &, const plexus::node_options &>,
              "omitting the transport is a compile error");
static_assert(!std::is_constructible_v<
                  inproc_node, inproc_executor<> &, plexus::discovery::static_discovery &,
                  const plexus::node_id &, inproc_transport<> &>,
              "omitting the options is a compile error");

// The node pins `this` into engine callbacks (the borrowed-substrate posture).
static_assert(!std::is_copy_constructible_v<inproc_node>);
static_assert(!std::is_move_constructible_v<inproc_node>);

TEST_CASE("node: constructs over the inproc substrate with a verbatim node_id", "[node][construction]")
{
    host h;
    const auto id = make_id(0xA1);
    inproc_node n{h.ex, h.disc, id, h.transport, make_opts()};

    REQUIRE(n.id() == id);
}

TEST_CASE("node: the escape hatches return the live engine objects", "[node][escape-hatch]")
{
    host h;
    inproc_node n{h.ex, h.disc, make_id(0xB2), h.transport, make_opts()};

    // router() is the live engine; message_forwarder() forwards router().messages().
    REQUIRE(&n.message_forwarder() == &n.router().messages());
    // The engine is reachable for advanced peer-level work (a const-correct read).
    const auto &const_n = n;
    REQUIRE(const_n.router().known().contains(make_id(0xCC)) == false);
    // executor() round-trips the borrowed executor by reference (same object).
    REQUIRE(&n.executor() == &h.ex);
}

TEST_CASE("node: the name-hash identity overload is deterministic and opt-in", "[node][identity]")
{
    host h1;
    host h2;
    host h3;

    inproc_node a{h1.ex, h1.disc, std::string_view{"sensor.front"}, h1.transport, make_opts()};
    inproc_node b{h2.ex, h2.disc, std::string_view{"sensor.front"}, h2.transport, make_opts()};
    inproc_node c{h3.ex, h3.disc, std::string_view{"sensor.rear"}, h3.transport, make_opts()};

    // Same name -> same identity (the documented opt-in property).
    REQUIRE(a.id() == b.id());
    // Distinct names -> distinct identities.
    REQUIRE(a.id() != c.id());
    // The derived id is not the zero id (the derivation actually mixed the name).
    REQUIRE(a.id() != plexus::node_id{});
}
