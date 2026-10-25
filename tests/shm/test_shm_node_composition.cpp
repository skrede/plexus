#include "plexus/asio/shm/local_shm_mux.h"
#include "plexus/asio/shm/shm_member.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/node_aliases.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/transport_backend.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <string>
#include <cstddef>
#include <optional>
#include <type_traits>

// The variadic-leaf shm node composition: a node<asio_policy, shm_member, unix_transport,
// asio_transport> builds its own multiplexer from the borrowed leaves (the consumer's
// preferred shape), and the node auto-installs the same-host preference hook because the
// pack carries an shm member. This guards the consumer's composition path — the seam the
// example demonstrates — at construction + same-host listen bring-up. The shm delivery data
// path is proven by the xproc round-trip suites (test_shm_same_host_roundtrip, etc.).

namespace pasio = plexus::asio;

static_assert(plexus::io::transport_backend<pasio::shm::local_shm_mux, plexus::muxify<pasio::asio_policy>>, "the lean crypto-free local_shm_mux must satisfy transport_backend");

TEST_CASE("shm.node_composition a variadic node composes shm+unix+tcp from its borrowed leaves", "[shm][mux][node]")
{
    using shm_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>;

    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};

    auto shm = pasio::shm::make_shm_member(io, broker);
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};

    plexus::node_id id{};
    id[0] = std::byte{0x2A};
    shm_node node{io, disc, id, shm, local, remote, plexus::node_options{}};

    SUCCEED("node<asio_policy, shm_member, unix_transport, asio_transport> instantiated and "
            "constructed");
}

TEST_CASE("shm.node_composition the shm-bearing node brings up a same-host listener", "[shm][mux][node]")
{
    using shm_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>;

    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};

    auto shm = pasio::shm::make_shm_member(io, broker);
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};

    plexus::node_id id{};
    id[0] = std::byte{0x3B};
    shm_node node{io, disc, id, shm, local, remote, plexus::node_options{}};

    const std::string sock = "/tmp/plexus-shm-node-comp-" + std::to_string(plexus::testing::process_id()) + ".sock";
    node.listen({"unix", sock});
    io.poll();

    SUCCEED("the shm-bearing node stood up a same-host listener");
}

namespace {

struct probe_codec
{
    using value_type = int;
    plexus::wire_bytes<> encode(const int &) const
    {
        return {};
    }
    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, int &) const
    {
        return {};
    }
    plexus::type_identity type_info() const
    {
        return {0x9B0BEu, "probe"};
    }
};

}

// Type-checks the generic advertise path on the shm-first pack, whose typed options carry the per-topic geometry, without
// standing up a live shm region; the xproc round-trip suites own the data-path provisioning.
template plexus::publisher<probe_codec> pasio::shm::node::advertise<probe_codec>(std::string_view, const plexus::typed_publisher_options &, probe_codec);

static_assert(std::is_same_v<decltype(plexus::typed_publisher_options::geometry), std::optional<plexus::io::same_host_geometry>>,
              "the per-topic geometry override must be a typed optional");

TEST_CASE("shm.node_composition the backend alias names the shm-first pack", "[shm][mux][node]")
{
    static_assert(std::is_same_v<pasio::shm::node, plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>>,
                  "plexus::asio::shm::node must name the shm-member-first composition pack");
    SUCCEED("plexus::asio::shm::node names the shm-member-first pack; its generic advertise path instantiates");
}
