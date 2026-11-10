#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/asio/shm/shm_member.h"

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/universe.h"
#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cstddef>

namespace {

namespace pasio = plexus::asio;

using shm_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>;

struct identity_fixture
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};
    pasio::shm::shm_member shm{pasio::shm::make_shm_member(io, broker)};
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
};

plexus::node_id make_id(std::byte first) noexcept
{
    plexus::node_id id{};
    id[0] = first;
    return id;
}

}

TEST_CASE("node_same_host_identity a node hands its own id and its universe to its shm member", "[node][shm][identity]")
{
    identity_fixture f;
    plexus::node_options opts;
    opts.report.universe = plexus::discovery::universe_from_label("identity.custom");
    REQUIRE(opts.report.universe != plexus::discovery::k_default_universe);

    const plexus::node_id id = make_id(std::byte{0x6E});
    shm_node node{f.io, f.disc, id, f.shm, f.local, f.remote, opts};

    REQUIRE(f.shm.self_id() == id);
    REQUIRE(f.shm.registry().universe() == opts.report.universe);
}

TEST_CASE("node_same_host_identity a node with no universe configured hands its member the default universe", "[node][shm][identity]")
{
    identity_fixture f;
    const plexus::node_id id = make_id(std::byte{0x6F});
    shm_node node{f.io, f.disc, id, f.shm, f.local, f.remote, plexus::node_options{}};

    REQUIRE(f.shm.self_id() == id);
    REQUIRE(f.shm.registry().universe() == plexus::discovery::k_default_universe);
}
