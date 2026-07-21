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

#include "plexus/target_profile.h"

#include "plexus/io/detail/forward_splice.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cstddef>
#include <type_traits>

namespace pasio = plexus::asio;

using leaf_multi_node  = plexus::node<pasio::asio_policy, pasio::unix_transport, pasio::asio_transport>;
using relay_multi_node = plexus::node<plexus::relay<pasio::asio_policy>, pasio::unix_transport, pasio::asio_transport>;

// The engine owns splice selection: a relay-spelled node threads the real forward_splice keyed on the
// ENGINE policy (a two-transport engine channel is polymorphic_byte_channel), while a non-relay node
// threads the byte-identical null twin that reaches no forwarding-send code.
static_assert(std::is_same_v<relay_multi_node::engine_type::forward_splice_type, plexus::io::forward_splice<relay_multi_node::engine_policy>>,
              "a relay-spelled node's engine must carry the real forward_splice keyed on the engine policy");
static_assert(std::is_same_v<leaf_multi_node::engine_type::forward_splice_type, plexus::io::null_forward_splice>,
              "a non-relay node's engine must carry the null forward-splice twin");

TEST_CASE("a variadic node composes a unix+tcp node from its borrowed leaves", "[integration][mux][node]")
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{{}};
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};

    plexus::node_id id{};
    id[0] = std::byte{0x2A};
    leaf_multi_node node{io, disc, id, local, remote, plexus::node_options{}};

    SUCCEED("node<asio_policy, unix_transport, asio_transport> instantiated and constructed");
}
