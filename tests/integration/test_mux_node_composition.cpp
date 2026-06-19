// The variadic node ctor builds its own multiplexer from the borrowed leaves (make_glue ->
// m_glue), distinct from the external-mux composition proven in test_mux_minimal_composition.
// This node<Policy, T1, T2, ...> shape is the one a downstream consumer instantiates, and its
// ctor member-init is the site that regressed under GCC 16.1.1 — guaranteed copy-elision is
// not applied into a [[no_unique_address]] member, so the by-value make_glue() prvalue
// demands the non-movable mux's deleted copy ctor. Instantiating + constructing the node here
// is the standing guard that the multi-transport node stays buildable on the GCC baseline.

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cstddef>

namespace pasio = plexus::asio;

TEST_CASE("a variadic node composes a unix+tcp node from its borrowed leaves",
          "[integration][mux][node]")
{
    using multi_node =
            plexus::node<pasio::asio_policy, pasio::unix_transport, pasio::asio_transport>;

    ::asio::io_context                  io;
    plexus::discovery::static_discovery disc{{}};
    pasio::unix_transport               local{io};
    pasio::asio_transport               remote{io};

    plexus::node_id id{};
    id[0] = std::byte{0x2A};
    multi_node node{io, disc, id, local, remote, plexus::node_options{}};

    SUCCEED("node<asio_policy, unix_transport, asio_transport> instantiated and constructed");
}
