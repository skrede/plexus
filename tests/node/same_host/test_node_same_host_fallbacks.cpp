#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/asio/shm/shm_member.h"

#include "plexus/io/upgrade_channel.h"

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cstddef>
#include <type_traits>

namespace {

namespace pasio = plexus::asio;

using shm_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>;

struct fallbacks_fixture
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};
    pasio::shm::shm_member shm{pasio::shm::make_shm_member(io, broker)};
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
};

}

TEST_CASE("node_same_host_fallbacks a node with an shm member exposes its same-host fallback counts by value, all zero at start", "[node][shm][same_host_fallbacks]")
{
    fallbacks_fixture f;
    plexus::node_id id{};
    id[0] = std::byte{0x5F};
    shm_node node{f.io, f.disc, id, f.shm, f.local, f.remote, plexus::node_options{}};

    static_assert(std::is_same_v<decltype(node.same_host_fallbacks()), plexus::io::fallback_counts>);
    const plexus::io::fallback_counts counts = node.same_host_fallbacks();
    REQUIRE(counts == plexus::io::fallback_counts{});
    REQUIRE(counts.size() == plexus::io::k_fallback_reason_count);
}
