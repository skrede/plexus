#include "plexus/asio/shm/linux/shm_member.h"
#include "plexus/asio/transport_set.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <string>
#include <cstddef>

// The owning composition bundle: a transport_set names its leaves once, constructs them in
// place, and mints a node that borrows them. This guards both the shm-bearing form (built
// from {io, broker}, the same shm+unix+tcp pack the node-composition test wires by hand) and
// the lean no-shm form (built from {io}), at construction + same-host listen bring-up.

namespace pasio = plexus::asio;

TEST_CASE("shm.transport_set an shm-bearing set mints a node from {io, broker}", "[shm][mux][node][transport_set]")
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};

    pasio::transport_set<pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport> ts{io, broker};

    plexus::node_id id{};
    id[0]     = std::byte{0x2A};
    auto node = ts.make_node<pasio::asio_policy>(disc, id, plexus::node_options{});

    const std::string sock = "/tmp/plexus-tset-shm-" + std::to_string(plexus::testing::process_id()) + ".sock";
    node.listen({"unix", sock});
    io.poll();

    SUCCEED("the shm-bearing transport_set minted a live node and stood up a same-host listener");
}

TEST_CASE("shm.transport_set a no-shm set mints a node from {io}", "[mux][node][transport_set]")
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{{}};

    pasio::transport_set<pasio::unix_transport, pasio::asio_transport> ts{io};

    plexus::node_id id{};
    id[0]     = std::byte{0x3B};
    auto node = ts.make_node<pasio::asio_policy>(disc, id, plexus::node_options{});

    const std::string sock = "/tmp/plexus-tset-noshm-" + std::to_string(plexus::testing::process_id()) + ".sock";
    node.listen({"unix", sock});
    io.poll();

    SUCCEED("the no-shm transport_set minted a live node and stood up a same-host listener");
}
