#include "plexus/node.h"
#include "plexus/post.h"
#include "plexus/policy.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"
#include "plexus/loopback_node.h"
#include "plexus/executor_verbs.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/process_loopback_channel.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <memory>
#include <cstddef>

namespace {

using asio_node = plexus::basic_node<plexus::asio::asio_policy, plexus::asio::asio_transport>;

struct inproc_loopback_policy : plexus::executor_verbs<plexus::inproc::inproc_executor<>>
{
    using byte_channel_type = plexus::io::process_loopback_channel<inproc_loopback_policy>;
    using timer_type        = plexus::inproc::inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;
};

static_assert(plexus::Policy<inproc_loopback_policy>);

}

TEST_CASE("post_refused plexus::post reports acceptance on asio and inproc nodes", "[node]")
{
    ::asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::discovery::static_discovery asio_disc{{}};
    asio_node node{io, asio_disc, plexus::node_id{std::byte{0x05}}, transport, plexus::node_options{}};
    REQUIRE(plexus::post(node, [] {}));
    io.poll();

    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    plexus::discovery::static_discovery inproc_disc{{}};
    plexus::loopback_host<inproc_loopback_policy> host{ex, inproc_disc, plexus::node_id{std::byte{0x06}}};
    REQUIRE(plexus::post(host.node(), [] {}));
    ex.drain();
}
